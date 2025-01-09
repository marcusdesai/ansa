use crate::handles::{Barrier, Consumer, Cursor, MultiProducer, SingleProducer};
use crate::ringbuffer::RingBuffer;
use crate::wait::WaitStrategy;
use crate::WaitBusy;
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
            panic!("Consumer ID: {id} already registered")
        }
        self.followed_by.entry(id).or_default();
        let follow_ids: &[usize] = match &follows {
            Follows::Producer => &[],
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
            wait_type: PhantomData,
        }
    }

    pub fn build_single_producer(self) -> (SingleProducer<E, W>, HashMap<usize, Consumer<E, W>>) {
        let producer_cursor = Arc::new(Cursor::new());
        let (consumers, cursor_map) = self.construct_consumers(&producer_cursor);
        let barrier = self.construct_producer_barrier(cursor_map);

        let producer = SingleProducer {
            cursor: producer_cursor,
            barrier,
            buffer: Arc::clone(&self.buffer),
            free_slots: 0,
            wait_strategy: (self.wait_factory)(),
        };

        (producer, consumers)
    }

    pub fn build_multi_producer(self) -> (MultiProducer<E, W>, HashMap<usize, Consumer<E, W>>) {
        let producer_cursor = Arc::new(Cursor::new());
        let (consumers, cursor_map) = self.construct_consumers(&producer_cursor);
        let barrier = self.construct_producer_barrier(cursor_map);

        let producer = MultiProducer {
            cursor: producer_cursor,
            barrier,
            buffer: Arc::clone(&self.buffer),
            claim: Arc::new(Cursor::new()),
            barrier_seq: 0,
            wait_strategy: (self.wait_factory)(),
        };

        (producer, consumers)
    }

    fn construct_producer_barrier(&self, cursor_map: UsizeMap<Arc<Cursor>>) -> Barrier {
        let mut cursors: Vec<_> = self
            .followed_by
            .iter()
            .filter(|(_, followed_by)| followed_by.is_empty())
            // unwrap okay as the cursor for this id must exist at this point
            .map(|(id, _)| Arc::clone(cursor_map.get(id).unwrap()))
            .collect();

        match cursors.len() {
            0 => panic!("No consumers follow the producer"),
            1 => Barrier::One(cursors.pop().unwrap()),
            _ => Barrier::Many(cursors.into_boxed_slice()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn construct_consumers(
        &self,
        producer_cursor: &Arc<Cursor>,
    ) -> (HashMap<usize, Consumer<E, W>>, UsizeMap<Arc<Cursor>>) {
        if self.follows.is_empty() {
            panic!("No consumers registered")
        }
        // Ensure no cycles in the consumer dag
        if let Some(cyclic_id) = find_cycle(&self.followed_by) {
            panic!("Cycle found for consumer id: {}", cyclic_id);
        }

        let mut consumers = HashMap::default();
        let mut cursor_map: UsizeMap<Arc<Cursor>> = UsizeMap::default();

        fn get_cursor(id: usize, map: &mut UsizeMap<Arc<Cursor>>) -> Arc<Cursor> {
            let cursor = map.entry(id).or_insert_with(|| Arc::new(Cursor::new()));
            Arc::clone(cursor)
        }

        for (&id, follows) in self.follows.iter() {
            let consumer_cursor = get_cursor(id, &mut cursor_map);
            let mut following_unregistered = None;

            let barrier = match follows {
                Follows::Producer => Barrier::One(Arc::clone(producer_cursor)),
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

// Uses topological sort, see: https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
fn find_cycle(graph: &UsizeMap<Vec<usize>>) -> Option<usize> {
    let mut visiting = UsizeSet::default();
    let mut visited = UsizeSet::default();

    fn visit(
        node: usize,
        visiting: &mut UsizeSet,
        visited: &mut UsizeSet,
        graph: &UsizeMap<Vec<usize>>,
    ) -> Option<usize> {
        if visited.contains(&node) {
            return None;
        }
        if visiting.contains(&node) {
            // cycle found
            return Some(node);
        }
        visiting.insert(node);
        // unwrap okay as we'll only use `find_cycle` when `node` is guaranteed to be in `graph`
        for neighbour in graph.get(&node).unwrap().iter() {
            if let cycle @ Some(_) = visit(*neighbour, visiting, visited, graph) {
                return cycle;
            }
        }
        visiting.remove(&node);
        visited.insert(node);
        None
    }

    for node in graph.keys() {
        if let cycle @ Some(_) = visit(*node, &mut visiting, &mut visited, graph) {
            return cycle;
        }
    }
    None
}

/// Describes the ordering relationship for a single consumer. `Follows::Consumers(vec![0, 1])`
/// denotes that a consumer reads elements on the ring buffer only after both `consumer 0` and
/// `consumer 1` have finished their reads.
pub enum Follows {
    Producer,
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
        assert_eq!(find_cycle(&graph), Some(0));
    }

    #[test]
    fn test_find_cycle_self_follow() {
        let graph = UsizeMap::from_iter([(0, vec![1]), (1, vec![1])]);
        assert_eq!(find_cycle(&graph), Some(1));
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
        assert_eq!(find_cycle(&graph), None);
    }
}
