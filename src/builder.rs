use crate::handles::{Barrier, Consumer, Cursor};
use crate::ringbuffer::RingBuffer;
use crate::wait::{WaitBlocking, WaitStrategy};
use crate::SingleProducer;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct DisruptorBuilder<E, WF, W> {
    buffer: Arc<RingBuffer<E>>,
    // factory function for building instances of the wait strategy, defaults to WaitBlocking
    wait_factory: WF,
    // Maps ids of consumers in a "followed by" relationship. For example, the pair  (3, [5, 7])
    // indicates that the consumer with id 3 is followed by the consumers with ids 5 and 7.
    followed_by: U64Map<Vec<u64>>,
    // The inverse of followed_by, useful for conveniently constructing barriers for consumers
    follows: U64Map<Follows>,
    // All the ids which follow the lead producer
    follows_lead: U64Set,
    // maps ids to handle type (producer or consumer)
    handles_map: U64Map<Handle>,
    wait_type: PhantomData<W>,
}

// Separate impl block for `new` so that a default type for wait factory can be provided
impl<E> DisruptorBuilder<E, fn() -> WaitBlocking, WaitBlocking> {
    pub fn new<F>(size: usize, element_factory: F) -> Self
    where
        E: Sync,
        F: FnMut() -> E,
    {
        DisruptorBuilder {
            buffer: Arc::new(RingBuffer::new(size, element_factory)),
            wait_factory: WaitBlocking::new,
            followed_by: U64Map::default(),
            follows: U64Map::default(),
            follows_lead: U64Set::default(),
            handles_map: U64Map::default(),
            wait_type: PhantomData,
        }
    }
}

impl<E, WF, W> DisruptorBuilder<E, WF, W>
where
    WF: Fn() -> W,
    W: WaitStrategy,
{
    pub fn add_handle(self, id: u64, h: Handle, f: Follows) -> Self {
        if self.follows.contains_key(&id) {
            panic!("id: {id} already registered")
        }
        unsafe { self.add_handle_unchecked(id, h, f) }
    }

    pub unsafe fn add_handle_unchecked(mut self, id: u64, h: Handle, f: Follows) -> Self {
        self.handles_map.insert(id, h);
        self.followed_by.entry(id).or_default();
        let follow_ids: &[u64] = match &f {
            Follows::LeadProducer => {
                self.follows_lead.insert(id);
                &[]
            }
            Follows::One(c) => &[*c],
            Follows::Many(cs) => cs,
        };
        follow_ids.iter().for_each(|c| {
            self.followed_by
                .entry(*c)
                .and_modify(|vec| vec.push(id))
                .or_insert_with(|| vec![id]);
        });
        self.follows.insert(id, f);
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
        if let Some((chain, id)) = validate_order(&self.handles_map, &chains) {
            panic!(
                "Chain of handles: {:?} unordered with respect to producer id: {}",
                chain, id
            )
        }
        for follows in self.follows.values() {
            let follow_ids: &[u64] = match follows {
                Follows::LeadProducer => &[],
                Follows::One(c) => &[*c],
                Follows::Many(cs) => cs,
            };
            for id in follow_ids {
                if !self.follows.contains_key(id) {
                    panic!("unregistered id: {id}")
                }
            }
        }
        unsafe { self.build_unchecked() }
    }

    pub unsafe fn build_unchecked(self) -> DisruptorHandles<E, W> {
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

    fn construct_lead_barrier(&self, cursor_map: U64Map<Arc<Cursor>>) -> Barrier {
        let mut cursors: Vec<_> = self
            .followed_by
            .iter()
            .filter(|(_, followed_by)| followed_by.is_empty())
            // unwrap okay as the cursor for this id must exist by this point
            .map(|(id, _)| Arc::clone(cursor_map.get(id).unwrap()))
            .collect();

        assert!(!cursors.is_empty());
        match cursors.len() {
            1 => Barrier::One(cursors.pop().unwrap()),
            _ => Barrier::Many(cursors.into_boxed_slice()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn construct_handles(
        &self,
        lead_cursor: &Arc<Cursor>,
    ) -> (
        U64Map<SingleProducer<E, W, false>>,
        U64Map<Consumer<E, W>>,
        U64Map<Arc<Cursor>>,
    ) {
        let mut producers = U64Map::default();
        let mut consumers = U64Map::default();
        let mut cursor_map = U64Map::default();

        fn get_cursor(id: u64, map: &mut U64Map<Arc<Cursor>>) -> Arc<Cursor> {
            let cursor = map.entry(id).or_insert_with(|| Arc::new(Cursor::new(0)));
            Arc::clone(cursor)
        }

        for (&id, follows) in self.follows.iter() {
            let handle_cursor = get_cursor(id, &mut cursor_map);

            let barrier = match follows {
                Follows::LeadProducer => Barrier::One(Arc::clone(lead_cursor)),
                Follows::One(follow_id) => Barrier::One(get_cursor(*follow_id, &mut cursor_map)),
                Follows::Many(many) => {
                    let follows_cursors = many
                        .iter()
                        .map(|follow_id| get_cursor(*follow_id, &mut cursor_map))
                        .collect();
                    Barrier::Many(follows_cursors)
                }
            };
            // unwrap okay as this entry in handles_map is guaranteed to exist for this id
            match self.handles_map.get(&id).unwrap() {
                Handle::Producer => {
                    let producer = SingleProducer {
                        cursor: handle_cursor,
                        barrier,
                        buffer: Arc::clone(&self.buffer),
                        free_slots: 0,
                        wait_strategy: (self.wait_factory)(),
                    };
                    producers.insert(id, producer);
                }
                Handle::Consumer => {
                    let consumer = Consumer {
                        cursor: handle_cursor,
                        barrier,
                        buffer: Arc::clone(&self.buffer),
                        wait_strategy: (self.wait_factory)(),
                    };
                    consumers.insert(id, consumer);
                }
            }
        }

        (producers, consumers, cursor_map)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DagError {
    Cycle(u64),
    Missing(u64),
    Disconnected(u64),
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

/// Returns every totally ordered chain of the partially ordered set which makes up the DAG, if
/// successful.
///
/// Otherwise, returns the relevant [`DagError`].
///
/// Finding the chains allows us to check whether all handles are totally ordered with respect to
/// all producers.
///
/// Uses DFS as purposed for topological sort, see:
/// https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
fn validate_graph(graph: &U64Map<Vec<u64>>, roots: &U64Set) -> Result<Vec<Vec<u64>>, DagError> {
    let mut visiting = U64Set::default();
    let mut visited = U64Set::default();
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

/// Helper function for recursively visiting the graph nodes using DFS.
fn visit(
    node: u64,
    visiting: &mut U64Set,
    visited: &mut U64Set,
    mut chain: Vec<u64>,
    graph: &U64Map<Vec<u64>>,
) -> Result<Vec<Vec<u64>>, DagError> {
    if visiting.contains(&node) {
        return Err(DagError::Cycle(node));
    }
    visiting.insert(node);

    chain.push(node);
    let mut chains = Vec::new();
    let children = graph.get(&node).ok_or(DagError::Missing(node))?;
    if children.is_empty() {
        chains.push(chain)
    } else {
        for child in children {
            let result = visit(*child, visiting, visited, chain.clone(), graph)?;
            chains.extend(result);
        }
    }

    visiting.remove(&node);
    visited.insert(node);
    Ok(chains)
}

/// Returns `None` if all chains contain all producers. Otherwise, returns the (chain, id) pair
/// where the producer id is not in the chain.
///
/// All chains must contain all producers to ensure that every node in the graph is totally ordered
/// with respect to all producers.
fn validate_order(handles_map: &U64Map<Handle>, chains: &Vec<Vec<u64>>) -> Option<(Vec<u64>, u64)> {
    let producer_ids: U64Set = handles_map
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
                return Some((chain.clone(), *id));
            }
        }
    }
    None
}

/// Describes the ordering relationship for a single handle.
///
/// `Follows::Many(vec![0, 1])` denotes that a handle interacts with elements on the ring buffer
/// only after both handles with ids `0` and `1` have finished their interactions.
#[derive(Debug)]
pub enum Follows {
    LeadProducer,
    One(u64),
    Many(Vec<u64>),
}

/// Describes the type of handle.
#[derive(Copy, Clone, Debug)]
pub enum Handle {
    Producer,
    Consumer,
}

/// Stores the producers and consumers for a single disruptor.
///
/// **Important**: It is likely a programming error to not use all the producers and consumers held
/// by this store. If any single producer or consumer fails to move on the ring buffer, then the
/// disruptor as a whole will eventually permanently stall.
///
/// When `debug_assertions` are enabled, a warning is printed if this struct is not empty when
/// dropped, i.e., there are still further producers or consumers to extract.
#[derive(Debug)]
pub struct DisruptorHandles<E, W> {
    lead: Option<SingleProducer<E, W, true>>,
    producers: U64Map<SingleProducer<E, W, false>>,
    consumers: U64Map<Consumer<E, W>>,
}

impl<E, W> DisruptorHandles<E, W> {
    /// Returns the lead producer when first called, and returns `None` on all subsequent calls.
    #[must_use = "Disruptor will stall if not used"]
    pub fn take_lead(&mut self) -> Option<SingleProducer<E, W, true>> {
        self.lead.take()
    }

    /// Returns the producer with this `id`. Returns `None` if this `id` has already been taken.
    #[must_use = "Disruptor will stall if not used"]
    pub fn take_producer(&mut self, id: u64) -> Option<SingleProducer<E, W, false>> {
        self.producers.remove(&id)
    }

    /// Returns the consumer with this `id`. Returns `None` if this `id` has already been taken.
    #[must_use = "Disruptor will stall if not used"]
    pub fn take_consumer(&mut self, id: u64) -> Option<Consumer<E, W>> {
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
struct HashU64(u64);

impl Hasher for HashU64 {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("`write` should never be called");
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type U64Map<V> = HashMap<u64, V, BuildHasherDefault<HashU64>>;
type U64Set = HashSet<u64, BuildHasherDefault<HashU64>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_cycle() {
        let graph = U64Map::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![0]),
            (4, vec![]),
        ]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Cycle(0)));

        let graph = U64Map::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![4]),
            (4, vec![2]),
        ]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Cycle(4)));
    }

    #[test]
    fn test_find_disconnected_branch() {
        // simulate where node 4 is disconnected from the graph
        let graph = U64Map::from_iter([
            (0, vec![1]),
            (1, vec![2]),
            (2, vec![3]),
            (3, vec![]),
            (4, vec![5]),
            (5, vec![4]),
        ]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(
            validate_graph(&graph, &roots),
            Err(DagError::Disconnected(4))
        );
    }

    #[test]
    fn test_find_missing_id() {
        let graph = U64Map::from_iter([(0, vec![1])]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Missing(1)));

        let graph = U64Map::from_iter([(0, vec![1, 2]), (1, vec![3]), (2, vec![3])]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Missing(3)));
    }

    #[test]
    fn test_find_cycle_self_follow() {
        let graph = U64Map::from_iter([(0, vec![1]), (1, vec![1])]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(validate_graph(&graph, &roots), Err(DagError::Cycle(1)));
    }

    #[test]
    fn test_find_chains() {
        let graph = U64Map::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![4]),
            (4, vec![]),
        ]);
        let roots = U64Set::from_iter([0]);
        let chains = vec![vec![0, 1, 3, 4], vec![0, 2, 4]];
        assert_eq!(validate_graph(&graph, &roots), Ok(chains));
    }

    #[test]
    fn test_find_chains_multiple_roots() {
        let graph = U64Map::from_iter([
            (0, vec![3]),
            (1, vec![4]),
            (2, vec![5]),
            (3, vec![6]),
            (4, vec![6]),
            (5, vec![6]),
            (6, vec![]),
        ]);
        let roots = U64Set::from_iter([0, 1, 2]);
        let chains = vec![vec![0, 3, 6], vec![1, 4, 6], vec![2, 5, 6]];
        assert_eq!(validate_graph(&graph, &roots), Ok(chains));
    }

    // 0 ─► 1 ─► 2 ─► 3 ─► 7
    // |              ▲
    // ▼              |
    // 4 ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_find_chains_fan_out_in_uneven() {
        let graph = U64Map::from_iter([
            (0, vec![1, 4]),
            (1, vec![2]),
            (2, vec![3]),
            (4, vec![5]),
            (5, vec![6]),
            (6, vec![3]),
            (3, vec![7]),
            (7, vec![]),
        ]);
        let roots = U64Set::from_iter([0]);
        let chains = vec![vec![0, 1, 2, 3, 7], vec![0, 4, 5, 6, 3, 7]];
        assert_eq!(validate_graph(&graph, &roots), Ok(chains));
    }

    // 0  ─► 1 ─► 2 ─► 3 ─► 7
    // |               ▲
    // ▼               |
    // 4P ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_producer_partial_order() {
        let handles_map = U64Map::from_iter([
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
        let result = validate_order(&handles_map, &chains);
        assert_eq!(result, Some((vec![0, 1, 2, 3, 7], 4)))
    }

    // 0 ─► 1 ─► 2 ─► 3P ─► 7
    // |              ▲
    // ▼              |
    // 4 ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_producer_total_order() {
        let handles_map = U64Map::from_iter([
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
        let result = validate_order(&handles_map, &chains);
        assert_eq!(result, None)
    }

    // 0P ─► 1 ─► 2 ─► 3 ─► 7
    // |               ▲
    // ▼               |
    // 4  ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_producer_total_order2() {
        let handles_map = U64Map::from_iter([
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
        let result = validate_order(&handles_map, &chains);
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
        let handles_map = U64Map::from_iter([
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
        let result = validate_order(&handles_map, &chains);
        assert_eq!(result, None)
    }

    // 0 ─► 1 ─► 2 ─► 3 ─► 4P
    // |
    // ▼
    // 5
    #[test]
    fn test_producer_partial_order2() {
        let handles_map = U64Map::from_iter([
            (0, Handle::Consumer),
            (1, Handle::Consumer),
            (2, Handle::Consumer),
            (3, Handle::Consumer),
            (4, Handle::Producer),
            (5, Handle::Consumer),
        ]);
        let chains = vec![vec![0, 1, 2, 3, 4], vec![0, 5]];
        let result = validate_order(&handles_map, &chains);
        assert_eq!(result, Some((vec![0, 5], 4)))
    }

    // 0 ─► 1 ─► 2 ─► 3 ─► 4
    // |
    // ▼
    // 5
    #[test]
    fn test_producer_total_order4() {
        let handles_map = U64Map::from_iter([
            (0, Handle::Consumer),
            (1, Handle::Consumer),
            (2, Handle::Consumer),
            (3, Handle::Consumer),
            (4, Handle::Consumer),
            (5, Handle::Consumer),
        ]);
        let chains = vec![vec![0, 1, 2, 3, 4], vec![0, 5]];
        let result = validate_order(&handles_map, &chains);
        assert_eq!(result, None)
    }
}
