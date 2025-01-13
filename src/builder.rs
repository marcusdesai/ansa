use crate::handles::{Barrier, Consumer, Cursor};
use crate::ringbuffer::RingBuffer;
use crate::wait::{WaitBusy, WaitStrategy};
use crate::SingleProducer;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct DisruptorBuilder<E, WF, W> {
    buffer: Arc<RingBuffer<E>>,
    // factory function for building instances of the wait strategy, defaults to WaitBusy
    wait_factory: WF,
    // Maps ids of consumers in a "followed by" relationship. For example, the pair  (3, [5, 7])
    // indicates that the consumer with id 3 is followed by the consumers with ids 5 and 7.
    followed_by: UsizeMap<Vec<usize>>,
    // The inverse of followed_by, useful for conveniently constructing barriers for consumers
    follows: UsizeMap<Follows>,
    // All the ids which follow the lead producer
    follows_lead: UsizeSet,
    // maps ids to handle type (producer or consumer)
    handles_map: UsizeMap<Handle>,
    wait_type: PhantomData<W>,
}

// Separate impl block for `new` so that a default type for wait factory can be provided
impl<E> DisruptorBuilder<E, fn() -> WaitBusy, WaitBusy> {
    pub fn new<F>(size: usize, element_factory: F) -> Self
    where
        E: Sync,
        F: FnMut() -> E,
    {
        DisruptorBuilder {
            buffer: Arc::new(RingBuffer::new(size, element_factory)),
            wait_factory: || WaitBusy,
            followed_by: UsizeMap::default(),
            follows: UsizeMap::default(),
            follows_lead: UsizeSet::default(),
            handles_map: UsizeMap::default(),
            wait_type: PhantomData,
        }
    }
}

impl<E, WF, W> DisruptorBuilder<E, WF, W>
where
    WF: Fn() -> W,
    W: WaitStrategy,
{
    pub fn add_consumer(mut self, id: usize, follows: Follows) -> Self {
        self.handles_map.insert(id, Handle::Consumer);
        self.add_handle(id, follows)
    }

    pub fn add_producer(mut self, id: usize, follows: Follows) -> Self {
        self.handles_map.insert(id, Handle::Producer);
        self.add_handle(id, follows)
    }

    fn add_handle(mut self, id: usize, follows: Follows) -> Self {
        if self.follows.contains_key(&id) {
            panic!("id: {id} already registered")
        }
        self.followed_by.entry(id).or_default();
        let follow_ids: &[usize] = match &follows {
            Follows::LeadProducer => {
                self.follows_lead.insert(id);
                &[]
            }
            Follows::Consumer(c) | Follows::Producer(c) => &[*c],
            Follows::Consumers(cs) => cs,
        };
        follow_ids.iter().for_each(|c| {
            self.followed_by
                .entry(*c)
                .and_modify(|vec| vec.push(id))
                .or_insert_with(|| vec![id]);
        });
        self.follows.insert(id, follows);
        self
    }

    pub fn wait_strategy<WF2, W2>(self, wait_factory: WF2) -> DisruptorBuilder<E, WF2, W2>
    where
        WF2: Fn() -> W2,
        W2: WaitStrategy,
    {
        DisruptorBuilder {
            buffer: self.buffer,
            wait_factory,
            followed_by: self.followed_by,
            follows: self.follows,
            follows_lead: self.follows_lead,
            handles_map: self.handles_map,
            wait_type: PhantomData,
        }
    }

    pub fn build(self) -> DisruptorHandles<E, W> {
        let lead_cursor = Arc::new(Cursor::new(0));
        let (producers, consumers, cursor_map) = self.construct_handles(&lead_cursor);
        let barrier = self.construct_lead_barrier(cursor_map);

        let lead_producer = SingleProducer {
            cursor: lead_cursor,
            barrier,
            buffer: Arc::clone(&self.buffer),
            free_slots: 0,
            wait_strategy: (self.wait_factory)(),
        };

        DisruptorHandles {
            lead: Some(lead_producer),
            producers,
            consumers,
        }
    }

    fn construct_lead_barrier(&self, cursor_map: UsizeMap<Arc<Cursor>>) -> Barrier {
        let mut cursors: Vec<_> = self
            .followed_by
            .iter()
            .filter(|(_, followed_by)| followed_by.is_empty())
            // unwrap okay as the cursor for this id must exist at this point
            .map(|(id, _)| Arc::clone(cursor_map.get(id).unwrap()))
            .collect();

        assert!(!cursors.is_empty());
        match cursors.len() {
            1 => Barrier::One(cursors.pop().unwrap()),
            _ => Barrier::Many(cursors.into_boxed_slice()),
        }
    }

    // use numbering of unordered layers to reason about trailing producers in the dag
    // consumers must be totally ordered with respect to producers
    #[allow(clippy::type_complexity)]
    fn construct_handles(
        &self,
        lead_cursor: &Arc<Cursor>,
    ) -> (
        UsizeMap<SingleProducer<E, W, false>>,
        UsizeMap<Consumer<E, W>>,
        UsizeMap<Arc<Cursor>>,
    ) {
        if self.follows.is_empty() {
            panic!("No ids registered")
        }
        if self.follows_lead.is_empty() {
            panic!("No ids follow the lead producer")
        }
        let chains = match validate_graph(&self.followed_by, &self.follows_lead) {
            Ok(chains) => chains,
            Err(dag_error) => dag_error.panic(),
        };
        if let Some(chain) = validate_producer_total_order(&self.handles_map, &chains) {
            panic!(
                "Chain of handles: {:?} does not pass through all producers",
                chain
            )
        }

        let producers = UsizeMap::default();
        let mut consumers = UsizeMap::default();
        let mut cursor_map = UsizeMap::default();

        fn get_cursor(id: usize, map: &mut UsizeMap<Arc<Cursor>>) -> Arc<Cursor> {
            let cursor = map.entry(id).or_insert_with(|| Arc::new(Cursor::new(0)));
            Arc::clone(cursor)
        }

        for (&id, follows) in self.follows.iter() {
            let consumer_cursor = get_cursor(id, &mut cursor_map);
            let mut following_unregistered = None;

            let barrier = match follows {
                Follows::LeadProducer => Barrier::One(Arc::clone(lead_cursor)),
                Follows::Consumer(follow_id) | Follows::Producer(follow_id) => {
                    if !self.follows.contains_key(follow_id) {
                        following_unregistered = Some(follow_id)
                    }
                    Barrier::One(get_cursor(*follow_id, &mut cursor_map))
                }
                Follows::Consumers(many) => {
                    let follows_cursors: Box<[_]> = many
                        .iter()
                        .map(|follow_id| {
                            if !self.follows.contains_key(follow_id) {
                                following_unregistered = Some(follow_id)
                            }
                            get_cursor(*follow_id, &mut cursor_map)
                        })
                        .collect();
                    Barrier::Many(follows_cursors)
                }
            };

            // Ensure all ids which this handle follows are registered
            if let Some(follow_id) = following_unregistered {
                panic!("Handle id: {} follows unregistered id: {}", id, follow_id)
            }

            let consumer = Consumer {
                cursor: consumer_cursor,
                barrier,
                buffer: Arc::clone(&self.buffer),
                wait_strategy: (self.wait_factory)(),
            };

            consumers.insert(id, consumer);
        }

        (producers, consumers, cursor_map)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DagError {
    Cycle(usize),
    Missing(usize),
    Disconnected(usize),
}

impl DagError {
    fn panic(self) -> ! {
        match self {
            DagError::Cycle(cyclic_id) => panic!("Cycle for id: {}", cyclic_id),
            DagError::Missing(missing_id) => panic!("id: {} missing from graph", missing_id),
            DagError::Disconnected(dis_id) => panic!("Disconnected id: {}", dis_id),
        }
    }
}

/// return Ok(layers) or Err(cycle index)
///
/// Uses topological sort, see: https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
///
/// finding the layers allows us to check two things. Whether any consumer shares a layer with a
/// trailing producer, and whether all consumers are totally ordered with respect to all producers.
fn validate_graph(
    graph: &UsizeMap<Vec<usize>>,
    roots: &UsizeSet,
) -> Result<Vec<Vec<usize>>, DagError> {
    let mut visiting = UsizeSet::default();
    let mut visited = UsizeSet::default();
    let mut chains = Vec::new();
    for node in roots {
        let result = visit(*node, &mut visiting, &mut visited, Vec::new(), graph)?;
        chains.extend(result)
    }
    for node in graph.keys() {
        if !visited.contains(node) {
            return Err(DagError::Disconnected(*node));
        }
    }
    Ok(chains)
}

/// Helper function for recursively visiting the graph nodes using DFS
fn visit(
    node: usize,
    visiting: &mut UsizeSet,
    visited: &mut UsizeSet,
    mut chain: Vec<usize>,
    graph: &UsizeMap<Vec<usize>>,
) -> Result<Vec<Vec<usize>>, DagError> {
    if visiting.contains(&node) {
        return Err(DagError::Cycle(node));
    }

    chain.push(node);
    let mut chains = Vec::new();

    visiting.insert(node);
    match graph.get(&node) {
        None => return Err(DagError::Missing(node)),
        Some(children) if children.is_empty() => chains.push(chain),
        Some(children) => {
            for child in children {
                let result = visit(*child, visiting, visited, chain.clone(), graph)?;
                chains.extend(result);
            }
        }
    }

    visiting.remove(&node);
    visited.insert(node);
    Ok(chains)
}

/// All chains must contain all producers to ensure that every node in the graph is totally ordered
/// with respect to all producers.
fn validate_producer_total_order(
    handles_map: &UsizeMap<Handle>,
    chains: &Vec<Vec<usize>>,
) -> Option<Vec<usize>> {
    let producer_ids: UsizeSet = handles_map
        .iter()
        .filter(|(_, h)| matches!(h, Handle::Producer))
        .map(|(id, _)| *id)
        .collect();
    if producer_ids.is_empty() {
        return None;
    }
    for chain in chains {
        for id in &producer_ids {
            if !chain.contains(id) {
                return Some(chain.clone());
            }
        }
    }
    None
}

/// Describes the ordering relationship for a single consumer.
///
/// `Follows::Consumers(vec![0, 1])` denotes that a consumer reads elements on the ring buffer only
/// after both `consumer 0` and `consumer 1` have finished their reads.
#[derive(Debug)]
pub enum Follows {
    LeadProducer,
    Producer(usize),
    Consumer(usize),
    Consumers(Vec<usize>),
}

#[derive(Copy, Clone, Debug)]
enum Handle {
    Producer,
    Consumer,
}

/// Stores the producers and consumers for a single disruptor.
///
/// **Important**: It is likely a programming error to not use all the producers and consumers held
/// by this store. If any single producer or consumer fails to move on the ring buffer, then the
/// disruptor as a whole will eventually permanently stall.
///
/// When `debug_assertions` is enabled, a warning is printed if this struct is not empty when
/// dropped, i.e., there are still further producers or consumers to extract.
#[derive(Debug)]
pub struct DisruptorHandles<E, W> {
    lead: Option<SingleProducer<E, W, true>>,
    producers: UsizeMap<SingleProducer<E, W, false>>,
    consumers: UsizeMap<Consumer<E, W>>,
}

impl<E, W> DisruptorHandles<E, W> {
    /// Returns the lead producer when first called, and returns `None` on all subsequent calls.
    #[must_use = "Disruptor will stall if not used."]
    pub fn take_lead(&mut self) -> Option<SingleProducer<E, W, true>> {
        self.lead.take()
    }

    /// Returns the producer with this `id`. Returns `None` if this `id` has already been taken.
    #[must_use = "Disruptor will stall if not used."]
    pub fn take_producer(&mut self, id: usize) -> Option<SingleProducer<E, W, false>> {
        self.producers.remove(&id)
    }

    /// Returns the consumer with this `id`. Returns `None` if this `id` has already been taken.
    #[must_use = "Disruptor will stall if not used."]
    pub fn take_consumer(&mut self, id: usize) -> Option<Consumer<E, W>> {
        self.consumers.remove(&id)
    }

    /// Returns `true` when no handles are left to take from this struct.
    pub fn is_empty(&self) -> bool {
        self.lead.is_none() && self.consumers.is_empty() && self.producers.is_empty()
    }
}

#[cfg(debug_assertions)]
impl<E, W> Drop for DisruptorHandles<E, W> {
    fn drop(&mut self) {
        if !self.is_empty() {
            println!("WARNING: DisruptorHandles not empty when dropped");
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct HashUsize(u64);

impl Hasher for HashUsize {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("`write` should never be called.");
    }

    fn write_usize(&mut self, i: usize) {
        self.0 = i as u64;
    }
}

type UsizeMap<V> = HashMap<usize, V, BuildHasherDefault<HashUsize>>;
type UsizeSet = HashSet<usize, BuildHasherDefault<HashUsize>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_cycle() {
        let graph = UsizeMap::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![0]),
            (4, vec![]),
        ]);
        let roots = UsizeSet::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Cycle(0)));

        let graph = UsizeMap::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![4]),
            (4, vec![2]),
        ]);
        let roots = UsizeSet::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Cycle(4)));
    }

    #[test]
    fn test_find_disconnected_branch() {
        // simulate where node 4 is disconnected from the graph
        let graph = UsizeMap::from_iter([
            (0, vec![1]),
            (1, vec![2]),
            (2, vec![3]),
            (3, vec![]),
            (4, vec![5]),
            (5, vec![4]),
        ]);
        let roots = UsizeSet::from_iter([0]);
        assert_eq!(
            validate_graph(&graph, &roots),
            Err(DagError::Disconnected(4))
        );
    }

    #[test]
    fn test_find_missing_id() {
        let graph = UsizeMap::from_iter([(0, vec![1])]);
        let roots = UsizeSet::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Missing(1)));

        let graph = UsizeMap::from_iter([(0, vec![1, 2]), (1, vec![3]), (2, vec![3])]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Missing(3)));
    }

    #[test]
    fn test_find_cycle_self_follow() {
        let graph = UsizeMap::from_iter([(0, vec![1]), (1, vec![1])]);
        let roots = UsizeSet::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Cycle(1)));
    }

    #[test]
    fn test_find_chains() {
        let graph = UsizeMap::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![4]),
            (4, vec![]),
        ]);
        let roots = UsizeSet::from_iter([0]);
        let chains = vec![vec![0, 1, 3, 4], vec![0, 2, 4]];
        assert_eq!(validate_graph(&graph, &roots), Ok(chains));
    }

    #[test]
    fn test_find_chains_multiple_roots() {
        let graph = UsizeMap::from_iter([
            (0, vec![3]),
            (1, vec![4]),
            (2, vec![5]),
            (3, vec![6]),
            (4, vec![6]),
            (5, vec![6]),
            (6, vec![]),
        ]);
        let roots = UsizeSet::from_iter([0, 1, 2]);
        let chains = vec![vec![0, 3, 6], vec![1, 4, 6], vec![2, 5, 6]];
        assert_eq!(validate_graph(&graph, &roots), Ok(chains));
    }

    // 0 ─► 1 ─► 2 ─► 3 ─► 7
    // |              ▲
    // ▼              |
    // 4 ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_find_chains_fan_out_in_uneven() {
        let graph = UsizeMap::from_iter([
            (0, vec![1, 4]),
            (1, vec![2]),
            (2, vec![3]),
            (4, vec![5]),
            (5, vec![6]),
            (6, vec![3]),
            (3, vec![7]),
            (7, vec![]),
        ]);
        let roots = UsizeSet::from_iter([0]);
        let chains = vec![vec![0, 1, 2, 3, 7], vec![0, 4, 5, 6, 3, 7]];
        assert_eq!(validate_graph(&graph, &roots), Ok(chains));
    }

    // 0  ─► 1 ─► 2 ─► 3 ─► 7
    // |               ▲
    // ▼               |
    // 4P ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_producer_partial_order() {
        let handles_map = UsizeMap::from_iter([
            (0, Handle::Consumer),
            (1, Handle::Consumer),
            (2, Handle::Consumer),
            (3, Handle::Consumer),
            (4, Handle::Producer),
            (5, Handle::Consumer),
            (6, Handle::Consumer),
            (7, Handle::Consumer),
        ]);
        let chains = vec![vec![0, 1, 2, 3, 7], vec![0, 4, 5, 6, 3, 7]];
        let result = validate_producer_total_order(&handles_map, &chains);
        assert_eq!(result, Some(vec![0, 1, 2, 3, 7]))
    }

    // 0 ─► 1 ─► 2 ─► 3P ─► 7
    // |              ▲
    // ▼              |
    // 4 ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_producer_total_order() {
        let handles_map = UsizeMap::from_iter([
            (0, Handle::Consumer),
            (1, Handle::Consumer),
            (2, Handle::Consumer),
            (3, Handle::Producer),
            (4, Handle::Consumer),
            (5, Handle::Consumer),
            (6, Handle::Consumer),
            (7, Handle::Consumer),
        ]);
        let chains = vec![vec![0, 1, 2, 3, 7], vec![0, 4, 5, 6, 3, 7]];
        let result = validate_producer_total_order(&handles_map, &chains);
        assert_eq!(result, None)
    }

    // 0P ─► 1 ─► 2 ─► 3 ─► 7
    // |               ▲
    // ▼               |
    // 4  ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_producer_total_order2() {
        let handles_map = UsizeMap::from_iter([
            (0, Handle::Producer),
            (1, Handle::Consumer),
            (2, Handle::Consumer),
            (3, Handle::Consumer),
            (4, Handle::Consumer),
            (5, Handle::Consumer),
            (6, Handle::Consumer),
            (7, Handle::Consumer),
        ]);
        let chains = vec![vec![0, 1, 2, 3, 7], vec![0, 4, 5, 6, 3, 7]];
        let result = validate_producer_total_order(&handles_map, &chains);
        assert_eq!(result, None)
    }

    // 0 ─► 1
    // |    |
    // ▼    ▼
    // 2 ─► 3P ─► 4
    //      |     |
    //      ▼     ▼
    //      5  ─► 6P
    #[test]
    fn test_producer_total_order3() {
        let handles_map = UsizeMap::from_iter([
            (0, Handle::Consumer),
            (1, Handle::Consumer),
            (2, Handle::Consumer),
            (3, Handle::Producer),
            (4, Handle::Consumer),
            (5, Handle::Consumer),
            (6, Handle::Producer),
        ]);
        let chains = vec![
            vec![0, 1, 3, 4, 6],
            vec![0, 1, 3, 5, 6],
            vec![0, 2, 3, 4, 6],
            vec![0, 2, 3, 5, 6],
        ];
        let result = validate_producer_total_order(&handles_map, &chains);
        assert_eq!(result, None)
    }
}
