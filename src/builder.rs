use crate::handles::{Barrier, Consumer, Cursor, ProducerBuilder};
use crate::ringbuffer::RingBuffer;
use crate::wait::{WaitBusy, WaitStrategy};
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
        if self.follows.contains_key(&id) {
            panic!("id: {id} already registered")
        }
        self.followed_by.entry(id).or_default();
        let follow_ids: &[usize] = match &follows {
            Follows::LeadProducer => {
                self.follows_lead.insert(id);
                &[]
            }
            Follows::Consumer(c) => &[*c],
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
            wait_type: PhantomData,
        }
    }

    pub fn build(self) -> (ProducerBuilder<E, W>, HashMap<usize, Consumer<E, W>>) {
        let lead_cursor = Arc::new(Cursor::new());
        let (consumers, cursor_map) = self.construct_handles(&lead_cursor);
        let barrier = self.construct_lead_barrier(cursor_map);

        let lead_producer = ProducerBuilder {
            cursor: lead_cursor,
            barrier,
            buffer: Arc::clone(&self.buffer),
            wait_strategy: (self.wait_factory)(),
        };

        (lead_producer, consumers)
    }

    fn construct_lead_barrier(&self, cursor_map: UsizeMap<Arc<Cursor>>) -> Barrier {
        let mut cursors: Vec<_> = self
            .followed_by
            .iter()
            .filter(|(_, followed_by)| followed_by.is_empty())
            // unwrap okay as the cursor for this id must exist at this point
            .map(|(id, _)| Arc::clone(cursor_map.get(id).unwrap()))
            .collect();

        assert!(cursors.len() >= 1);
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
    ) -> (HashMap<usize, Consumer<E, W>>, UsizeMap<Arc<Cursor>>) {
        if self.follows.is_empty() {
            panic!("No ids registered")
        }
        if self.follows_lead.is_empty() {
            panic!("No ids follow the lead producer")
        }
        let _ = match find_graph_layers(&self.followed_by, &self.follows_lead) {
            Ok(layers) => layers,
            Err(cyclic_id) => panic!("Cycle found for id: {}", cyclic_id),
        };

        let mut consumers = HashMap::default();
        let mut cursor_map = UsizeMap::default();

        fn get_cursor(id: usize, map: &mut UsizeMap<Arc<Cursor>>) -> Arc<Cursor> {
            let cursor = map.entry(id).or_insert_with(|| Arc::new(Cursor::new()));
            Arc::clone(cursor)
        }

        for (&id, follows) in self.follows.iter() {
            let consumer_cursor = get_cursor(id, &mut cursor_map);
            let mut following_unregistered = None;

            let barrier = match follows {
                Follows::LeadProducer => Barrier::One(Arc::clone(lead_cursor)),
                Follows::Consumer(follow_id) => {
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

            // Ensure all ids which this consumer follows are registered
            if let Some(follow_id) = following_unregistered {
                panic!("Consumer id: {} follows unregistered id: {}", id, follow_id)
            }

            let consumer = Consumer {
                cursor: consumer_cursor,
                barrier,
                buffer: Arc::clone(&self.buffer),
                wait_strategy: (self.wait_factory)(),
            };

            consumers.insert(id, consumer);
        }

        (consumers, cursor_map)
    }
}

/// return Ok(layers) or Err(cycle index)
///
/// Uses topological sort, see: https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
///
/// finding the layers allows us to check two things. Whether any consumer shares a layer with a
/// trailing producer, and whether all consumers are totally ordered with respect to all producers.
///
/// Unfortunately, the current implementation results in potentially visiting each node multiple
/// times, but this is necessary to ensure we calculate the layers correctly.
fn find_graph_layers(
    graph: &UsizeMap<Vec<usize>>,
    roots: &UsizeSet,
) -> Result<Vec<Vec<usize>>, usize> {
    let mut visiting = UsizeSet::default();
    let mut layers = UsizeMap::default(); // maps nodes -> which layer they inhabit

    fn visit(
        node: usize,
        layer: usize,
        visiting: &mut UsizeSet,
        layers: &mut UsizeMap<usize>,
        graph: &UsizeMap<Vec<usize>>,
    ) -> Option<usize> {
        layers
            .entry(node)
            .and_modify(|l| *l = (*l).max(layer))
            .or_insert(layer);

        if visiting.contains(&node) {
            // cycle found
            return Some(node);
        }

        visiting.insert(node);
        // unwrap okay as we'll only use `find_cycle` when `node` is guaranteed to be in `graph`
        for child in graph.get(&node).unwrap().iter() {
            if let cycle @ Some(_) = visit(*child, layer + 1, visiting, layers, graph) {
                return cycle;
            }
        }
        visiting.remove(&node);
        None
    }

    for node in roots {
        if let Some(cycle) = visit(*node, 0, &mut visiting, &mut layers, graph) {
            return Err(cycle);
        }
    }

    // invert the index
    let mut layers_inverted: UsizeMap<Vec<usize>> = UsizeMap::default();
    for (node_id, layer) in layers {
        layers_inverted
            .entry(layer)
            .and_modify(|l| l.push(node_id))
            .or_insert_with(|| vec![node_id]);
    }

    let layers_vec = (0..layers_inverted.len())
        // unwrap okay due to above construction of layers_inverted
        .map(|layer| layers_inverted.remove(&layer).unwrap())
        .collect();

    Ok(layers_vec)
}

// fn ensure_producer_total_order() {}

/// Describes the ordering relationship for a single consumer.
///
/// `Follows::Consumers(vec![0, 1])` denotes that a consumer reads elements on the ring buffer only
/// after both `consumer 0` and `consumer 1` have finished their reads.
pub enum Follows {
    LeadProducer,
    Consumer(usize),
    Consumers(Vec<usize>),
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
        assert_eq!(find_graph_layers(&graph, &roots), Err(0));
    }

    #[test]
    fn test_find_cycle_self_follow() {
        let graph = UsizeMap::from_iter([(0, vec![1]), (1, vec![1])]);
        let roots = UsizeSet::from_iter([0]);
        assert_eq!(find_graph_layers(&graph, &roots), Err(1));
    }

    #[test]
    fn test_find_no_cycle() {
        let graph = UsizeMap::from_iter([
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![4]),
            (3, vec![4]),
            (4, vec![]),
        ]);
        let roots = UsizeSet::from_iter([0]);
        let layers = vec![vec![0], vec![1, 2], vec![3], vec![4]];
        assert_eq!(find_graph_layers(&graph, &roots), Ok(layers));
    }

    #[test]
    fn test_multiple_roots() {
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
        let layers = vec![vec![0, 1, 2], vec![3, 4, 5], vec![6]];
        assert_eq!(find_graph_layers(&graph, &roots), Ok(layers));
    }

    // 0 ─► 1 ─► 2 ─► 3 ─► 7
    // |              ▲
    // ▼              |
    // 4 ─► 5 ─► 6 ─-─┘
    #[test]
    fn test_find_layers_fan_out_in_uneven() {
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
        let layers = vec![vec![0], vec![1, 4], vec![2, 5], vec![6], vec![3], vec![7]];
        assert_eq!(find_graph_layers(&graph, &roots), Ok(layers));
    }
}
