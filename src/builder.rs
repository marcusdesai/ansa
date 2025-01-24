//! todo: Examples like fan out, fan in, etc...

use crate::handles::{Barrier, Consumer, Cursor, Producer};
use crate::ringbuffer::RingBuffer;
use crate::wait::{WaitBlocking, WaitStrategy};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{BuildHasherDefault, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

/// Configures and builds the buffer and handles for a single disruptor.
///
/// The configuration of the disruptor is only evaluated when [`build`](DisruptorBuilder::build)
/// is called.
#[derive(Debug)]
pub struct DisruptorBuilder<F, E, WF, W> {
    /// Size of the ring buffer, must be power of 2.
    buffer_size: usize,
    /// Factory function for populating the buffer with elements. Wrapped in an option only to
    /// facilitate moving the value out of the builder while the builder is still in use.
    element_factory: Option<F>,
    /// Factory function for building instances of the wait strategy, defaults to WaitBlocking.
    wait_factory: WF,
    /// Maps ids of consumers in a "followed by" relationship. For example, the pair  `(3, [5, 7])`
    /// indicates that the consumer with id `3` is followed by the consumers with ids `5` and `7`.
    followed_by: U64Map<Vec<u64>>,
    /// The inverse of followed_by, useful for conveniently constructing barriers for consumers.
    follows: U64Map<Follows>,
    /// All the ids which follow the lead producer. These are the roots of the graph.
    follows_lead: U64Set,
    /// Maps ids to handle type (producer or consumer).
    handles_map: U64Map<Handle>,
    /// Tracks whether ids have been reused.
    overlapping_ids: U64Set,
    element_type: PhantomData<E>,
    wait_type: PhantomData<W>,
}

// separate impl block for `new` so that a default type for wait factory can be provided
impl<F, E> DisruptorBuilder<F, E, fn() -> WaitBlocking, WaitBlocking> {
    /// Returns a new [`DisruptorBuilder`].
    ///
    /// `size` must be a non-zero power of 2.
    ///
    /// The default wait factory returns instances of: [`WaitBlocking`].
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// // with a very simple element_factory
    /// let builder = DisruptorBuilder::new(64, || 0i64);
    ///
    /// // using a closure to capture state
    /// let mut counter = 0;
    /// let factory = move || {
    ///     let ret = counter;
    ///     counter += 1;
    ///     ret
    /// };
    /// let builder = DisruptorBuilder::new(64, factory);
    /// ```
    pub fn new(size: usize, element_factory: F) -> Self
    where
        E: Sync,
        F: FnMut() -> E,
    {
        DisruptorBuilder {
            buffer_size: size,
            element_factory: Some(element_factory),
            wait_factory: WaitBlocking::new,
            followed_by: U64Map::default(),
            follows: U64Map::default(),
            follows_lead: U64Set::default(),
            handles_map: U64Map::default(),
            overlapping_ids: U64Set::default(),
            element_type: PhantomData,
            wait_type: PhantomData,
        }
    }
}

impl<F, E, WF, W> DisruptorBuilder<F, E, WF, W>
where
    F: FnMut() -> E,
    WF: Fn() -> W,
    W: WaitStrategy,
{
    /// Add a handle to the disruptor.
    ///
    /// Each handle must be associated with a unique id, and must be either a consumer or producer.
    ///
    /// Every handle must describe how it relates to other handles by indicating which handles it
    /// follows. In the disruptor pattern, the management of access to the underlying ring buffer
    /// is organised by ordering handles according to which handles come before.
    ///
    /// The handles of a disruptor must create a directed acyclic graph, where each handle follows
    /// either the lead producer, or other handles. The "lead producer" is implicitly defined
    /// simply by creating the ['DisruptorBuilder']. This lead serves as the root of the graph,
    /// everything else must in some way follow it, and it will always be a producer.
    ///
    /// # Examples
    ///
    /// Consumer handles do not need to necessarily follow one another, which allows for them to
    /// overlap reads of the ring buffer.
    /// ```
    /// use ansa::*;
    ///
    /// // lead ─► 0
    /// //    |
    /// //    ▼
    /// //    1
    /// let _ = DisruptorBuilder::new(0, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::LeadProducer);
    /// ```
    /// This represents a fan-out from the lead producer to the following consumers `0` and `1`.
    ///
    /// But all producers must always follow or be followed by other handles, as there cannot be
    /// overlapping writes of the buffer.
    /// ```
    /// use ansa::*;
    ///
    /// // lead ─► 0 ─► 2
    /// //         |    |
    /// //         ▼    ▼
    /// //         1 ─► 3P ─► 4
    /// let _ = DisruptorBuilder::new(0, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .add_handle(2, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .add_handle(3, Handle::Producer, Follows::Handles(vec![1, 2])) // fan-in
    ///     .add_handle(4, Handle::Consumer, Follows::Handles(vec![3]));
    /// ```
    ///
    /// Another way of expressing this is that: it is possible to add consumer handles which are
    /// partially ordered with respect to other consumer handles, but all handles (of any kind)
    /// must be totally ordered with respect to all producer handles.
    ///
    /// See: [`BuildError`], for full details on error states encountered when building the
    /// disruptor. Such invalid states are most often caused by misconfigured calls to
    /// [`add_handle`](DisruptorBuilder::add_handle) or
    /// [`extend_handles`](DisruptorBuilder::extend_handles).
    pub fn add_handle(mut self, id: u64, handle: Handle, follows: Follows) -> Self {
        if self.follows.contains_key(&id) {
            self.overlapping_ids.insert(id);
        }
        self.handles_map.insert(id, handle);
        self.followed_by.entry(id).or_default();
        match &follows {
            Follows::LeadProducer => {
                self.follows_lead.insert(id);
            }
            Follows::Handles(cs) => cs.iter().for_each(|c| {
                self.followed_by
                    .entry(*c)
                    .and_modify(|vec| vec.push(id))
                    .or_insert_with(|| vec![id]);
            }),
        }
        self.follows.insert(id, follows);
        self
    }

    /// Extend the handles graph with an iterator.
    ///
    /// The implementation simply calls [`add_handle`](DisruptorBuilder::add_handle) on each
    /// element of the iterator.
    ///
    /// # Examples
    ///
    ///```
    /// use ansa::*;
    ///
    /// let _ = DisruptorBuilder::new(64, || 0)
    ///    .extend_handles([
    ///        (0, Handle::Consumer, Follows::LeadProducer),
    ///        (1, Handle::Consumer, Follows::LeadProducer),
    ///        (2, Handle::Consumer, Follows::Handles(vec![0, 1]))
    ///    ])
    ///    .build();
    ///```
    pub fn extend_handles(self, iter: impl IntoIterator<Item = (u64, Handle, Follows)>) -> Self {
        let mut this = self;
        for (id, handle, follows) in iter.into_iter() {
            this = this.add_handle(id, handle, follows);
        }
        this
    }

    /// Set the wait strategy to be used by all consumers and producers.
    ///
    /// For details of all provided wait strategies, see the module docs for [`wait`](crate::wait).
    pub fn wait_strategy<WF2, W2>(self, factory: WF2) -> DisruptorBuilder<F, E, WF2, W2>
    where
        WF2: Fn() -> W2,
        W2: WaitStrategy,
    {
        DisruptorBuilder {
            buffer_size: self.buffer_size,
            element_factory: self.element_factory,
            wait_factory: factory,
            followed_by: self.followed_by,
            follows: self.follows,
            follows_lead: self.follows_lead,
            handles_map: self.handles_map,
            overlapping_ids: self.overlapping_ids,
            element_type: PhantomData,
            wait_type: PhantomData,
        }
    }

    /// Returns the constructed [`DisruptorHandles`] struct if successful, otherwise returns
    /// [`BuildError`].
    ///
    /// For full details on error states, please see [`BuildError`].
    pub fn build(mut self) -> Result<DisruptorHandles<E, W>, BuildError> {
        self.validate()?;
        // unwrap okay as this value will always be inhabited as per construction of the builder
        let element_factory = self.element_factory.take().unwrap();
        let buffer = Arc::new(RingBuffer::new(self.buffer_size, element_factory));
        let lead_cursor = Arc::new(Cursor::start());
        let (producers, consumers, cursor_map) = self.construct_handles(&lead_cursor, &buffer);
        let barrier = self.construct_lead_barrier(cursor_map);

        let lead_producer = Producer {
            cursor: lead_cursor,
            barrier,
            buffer,
            free_slots: 0,
            wait_strategy: (self.wait_factory)(),
        };

        Ok(DisruptorHandles {
            lead: Some(lead_producer),
            producers,
            consumers,
        })
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
        buffer: &Arc<RingBuffer<E>>,
    ) -> (
        U64Map<Producer<E, W, false>>,
        U64Map<Consumer<E, W>>,
        U64Map<Arc<Cursor>>,
    ) {
        let mut producers = U64Map::default();
        let mut consumers = U64Map::default();
        let mut cursor_map = U64Map::default();

        fn get_cursor(id: u64, map: &mut U64Map<Arc<Cursor>>) -> Arc<Cursor> {
            // start cursors at -1 to make buffer accesses zero indexed from the start
            let cursor = map.entry(id).or_insert_with(|| Arc::new(Cursor::start()));
            Arc::clone(cursor)
        }

        for (&id, follows) in self.follows.iter() {
            let cursor = get_cursor(id, &mut cursor_map);

            let barrier = match follows {
                Follows::LeadProducer => Barrier::One(Arc::clone(lead_cursor)),
                Follows::Handles(ids) if ids.len() == 1 => {
                    Barrier::One(get_cursor(ids[0], &mut cursor_map))
                }
                Follows::Handles(ids) => {
                    let follows_cursors = ids
                        .iter()
                        .map(|follow_id| get_cursor(*follow_id, &mut cursor_map))
                        .collect();
                    Barrier::Many(follows_cursors)
                }
            };
            // unwrap okay as this entry in handles_map is guaranteed to exist for this id
            match self.handles_map.get(&id).unwrap() {
                Handle::Producer => {
                    let producer = Producer {
                        cursor,
                        barrier,
                        buffer: Arc::clone(buffer),
                        free_slots: 0,
                        wait_strategy: (self.wait_factory)(),
                    };
                    producers.insert(id, producer);
                }
                Handle::Consumer => {
                    let consumer = Consumer {
                        cursor,
                        barrier,
                        buffer: Arc::clone(buffer),
                        wait_strategy: (self.wait_factory)(),
                    };
                    consumers.insert(id, consumer);
                }
            }
        }

        (producers, consumers, cursor_map)
    }

    fn validate(&self) -> Result<(), BuildError> {
        if self.follows.is_empty() {
            return Err(BuildError::EmptyGraph);
        }
        for id in self.followed_by.keys() {
            if !self.follows.contains_key(id) {
                return Err(BuildError::UnregisteredID(*id));
            }
        }
        let size = self.buffer_size;
        if size == 0 || (size & (size - 1)) != 0 {
            return Err(BuildError::BufferSize(size));
        }
        if !self.overlapping_ids.is_empty() {
            return Err(BuildError::OverlappingIDs(
                self.overlapping_ids.iter().copied().collect(),
            ));
        }
        let chains = validate_graph(&self.followed_by, &self.follows_lead)?;
        validate_order(&self.handles_map, &chains)?;
        Ok(())
    }
}

/// Describes the possible errors encountered when building a disruptor.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BuildError {
    /// A buffer size which failed the non-zero power of 2 requirement.
    BufferSize(usize),
    /// A list of ids which have been added multiple times.
    OverlappingIDs(Vec<u64>),
    /// Indicates that no handles have been added to the disruptor.
    EmptyGraph,
    /// Indicates that some handles are not ordered with respect to this producer id. Unordered
    /// producers may overlap buffer accesses with other handles, causing Undefined behaviour due
    /// to mutable aliasing.
    ///
    /// ## Example Causes
    /// ```
    /// use ansa::*;
    ///
    /// let result = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(2, Handle::Producer, Follows::Handles(vec![0]))
    ///     .build();
    ///
    /// assert_eq!(result.err().unwrap(), BuildError::UnorderedProducer(vec![1], 2));
    /// ```
    /// Since producer `2` does not explicitly follow consumer `1`, it cannot be guaranteed that
    /// their buffer access do not overlap. We can fix this by adding id `1` to the `Follows` vec
    /// for producer `2`.
    ///
    /// ```
    /// use ansa::*;
    ///
    /// let result = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .extend_handles([
    ///         (2, Handle::Consumer, Follows::Handles(vec![0])),
    ///         (3, Handle::Consumer, Follows::Handles(vec![2])),
    ///         (4, Handle::Consumer, Follows::Handles(vec![3])),
    ///         (5, Handle::Producer, Follows::Handles(vec![4])),
    ///     ])
    ///     .build();
    ///
    /// assert_eq!(result.err().unwrap(), BuildError::UnorderedProducer(vec![0, 1], 5));
    /// ```
    /// Even though it may appear very likely that consumer `1` will finish before producer `5`, it
    /// cannot be guaranteed. This can be fixed by adding id `1` to the `Follows` vec for producer
    /// `5`.
    ///
    /// ```
    /// use ansa::*;
    ///
    /// // defines the following graph:
    /// // lead ─► 0 ─► 1P ─► 2 ─► 3 ─► 7
    /// //         |                    ▲
    /// //         ▼                    |
    /// //         4 ─► 5 ─► 6 ─-------─┘
    /// let result = DisruptorBuilder::new(64, || 0)
    ///     .extend_handles([
    ///         (0, Handle::Consumer, Follows::LeadProducer),
    ///         (1, Handle::Producer, Follows::Handles(vec![0])),
    ///         (2, Handle::Consumer, Follows::Handles(vec![1])),
    ///         (3, Handle::Consumer, Follows::Handles(vec![2])),
    ///     ])
    ///     .extend_handles([
    ///         (4, Handle::Consumer, Follows::Handles(vec![0])),
    ///         (5, Handle::Consumer, Follows::Handles(vec![4])),
    ///         (6, Handle::Consumer, Follows::Handles(vec![5])),
    ///         (7, Handle::Consumer, Follows::Handles(vec![3, 6])),
    ///     ])
    ///     .build();
    ///
    /// assert_eq!(result.err().unwrap(), BuildError::UnorderedProducer(vec![0, 4, 5, 6, 7], 1));
    /// ```
    /// In the second call to `extend_handles`, the handles `[4, 5, 6]` are not ordered with respect
    /// to producer `1`. This means we cannot guarantee that those handles and producer `1` will not
    /// overlap buffer accesses. The fix here is not so obvious, but any addition of a `Follows`
    /// ordering so that every id in `[4, 5, 6]` is ordered with id `1` will resolve the error.
    UnorderedProducer(Vec<u64>, u64),
    /// An ID which is referred to in the graph, but has not been added explicitly.
    ///
    /// ## Example Cause
    /// ```
    /// use ansa::*;
    ///
    /// let result = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0, 2])) // <- here
    ///     .build();
    ///
    /// assert_eq!(result.err().unwrap(), BuildError::UnregisteredID(2));
    /// ```
    UnregisteredID(u64),
    /// An ID which loops in the graph. Loops between handles will cause the disruptor to deadlock.
    ///
    /// ## Example Cause
    /// ```
    /// use ansa::*;
    ///
    /// let result = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .add_handle(2, Handle::Consumer, Follows::Handles(vec![1, 3])) // <- here
    ///     .add_handle(3, Handle::Consumer, Follows::Handles(vec![2]))
    ///     .build();
    ///
    /// assert_eq!(result.err().unwrap(), BuildError::GraphCycle(2));
    /// ```
    GraphCycle(u64),
    /// An ID which is disconnected from the rest of the graph. Disconnected in this context means
    /// this id is not reachable from the root of the graph when traversing the follows relationships.
    ///
    /// ## Example Cause
    /// ```
    /// use ansa::*;
    ///
    /// let result = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![])) // <- here
    ///     .build();
    ///
    /// assert_eq!(result.err().unwrap(), BuildError::DisconnectedNode(1));
    /// ```
    DisconnectedNode(u64),
}

impl Display for BuildError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            BuildError::BufferSize(size) => {
                format!("RingBuffer size must be a non-zero power of 2; given size: {size}")
            }
            BuildError::OverlappingIDs(ids) => format!("Found overlapping ids: {ids:?}"),
            BuildError::EmptyGraph => "Graph empty, no handles added".to_owned(),
            BuildError::UnorderedProducer(chain, id) => {
                format!("Chain of handle ids: {chain:?} does not contain producer id: {id}")
            }
            BuildError::UnregisteredID(id) => format!("Unregistered id: {id} referred to in graph"),
            BuildError::GraphCycle(id) => format!("Cycle in graph for id: {id}"),
            BuildError::DisconnectedNode(id) => format!("id: {id} disconnected from graph"),
        };
        write!(f, "{}", str)
    }
}

impl Error for BuildError {}

/// Checks whether the given graph is a fully connected Directed Acyclic Graph.
///
/// Returns every totally ordered chain of the partially ordered set which makes up the DAG, if
/// successful. Otherwise, returns the relevant [`BuildError`].
///
/// Finding the chains allows us to check whether all handles are totally ordered with respect to
/// all producers.
fn validate_graph(graph: &U64Map<Vec<u64>>, roots: &U64Set) -> Result<Vec<Vec<u64>>, BuildError> {
    let mut visiting = U64Set::default();
    let mut visited = U64Set::default();
    let mut chains = Vec::new();
    for node in roots {
        let result = visit(*node, &mut visiting, &mut visited, Vec::new(), graph)?;
        chains.extend(result)
    }
    for node in graph.keys() {
        if !visited.contains(node) {
            return Err(BuildError::DisconnectedNode(*node));
        }
    }
    Ok(chains)
}

/// Helper function for recursively visiting the graph nodes using DFS.
///
/// Uses DFS as usually purposed for topological sort, see:
/// https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
fn visit(
    node: u64,
    visiting: &mut U64Set,
    visited: &mut U64Set,
    mut chain: Vec<u64>,
    graph: &U64Map<Vec<u64>>,
) -> Result<Vec<Vec<u64>>, BuildError> {
    if visiting.contains(&node) {
        return Err(BuildError::GraphCycle(node));
    }
    visiting.insert(node);

    chain.push(node);
    let mut chains = Vec::new();
    let children = graph.get(&node).ok_or(BuildError::UnregisteredID(node))?;
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

/// Returns `Ok` if all chains of the graph contain all producers. Otherwise, returns the
/// `(chain, id)` pair where the producer id is not in the chain.
///
/// A chain is a totally-ordered subset of a partially-ordered set (poset). If a chain does not
/// include an element, `e`, of the poset, then there exist elements of that chain which are
/// unordered with respect to `e`. Thus, to ensure that every element in the graph is totally
/// ordered with respect to all producers, it must hold that all chains contain all producers.
///
/// Another way of thinking about this is: it should not be possible to traverse the DAG without
/// visiting all producers.
fn validate_order(handles_map: &U64Map<Handle>, chains: &Vec<Vec<u64>>) -> Result<(), BuildError> {
    let producer_ids: U64Set = handles_map
        .iter()
        .filter(|(_, h)| matches!(h, Handle::Producer))
        .map(|(id, _)| *id)
        .collect();
    if producer_ids.is_empty() {
        return Ok(());
    }
    for chain in chains {
        for id in &producer_ids {
            if !chain.contains(id) {
                return Err(BuildError::UnorderedProducer(chain.clone(), *id));
            }
        }
    }
    Ok(())
}

/// Describes the ordering relationship for a single handle.
///
/// # Examples
/// ```
/// use ansa::*;
///
/// // ordered directly after the lead producer
/// let _ = Follows::LeadProducer;
///
/// // ordered directly after the handle with id `0`
/// let _ = Follows::Handles(vec![0]);
///
/// // ordered directly after the handles with ids `0`, `1` and `2`
/// let _ = Follows::Handles(vec![0, 1, 2]); // fan-in
/// ```
/// When one handles is 'ordered directly' after another, it will interact with a sequence on the
/// buffer only after all handles it follows have concluded their interactions with that sequence.
///
/// In disruptor terminology, the cursors of all handles ordered directly before handle `A` will
/// together form the barrier for `A`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Follows {
    LeadProducer,
    Handles(Vec<u64>),
}

/// Describes the type of handle.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
    lead: Option<Producer<E, W, true>>,
    producers: U64Map<Producer<E, W, false>>,
    consumers: U64Map<Consumer<E, W>>,
}

impl<E, W> DisruptorHandles<E, W> {
    /// Returns the lead producer when first called, and returns `None` on all subsequent calls.
    #[must_use = "Disruptor will stall if any handle is not used "]
    pub fn take_lead(&mut self) -> Option<Producer<E, W, true>> {
        self.lead.take()
    }

    /// Returns the producer with this `id`. Returns `None` if this `id` has already been taken.
    #[must_use = "Disruptor will stall if any handle is not used"]
    pub fn take_producer(&mut self, id: u64) -> Option<Producer<E, W, false>> {
        self.producers.remove(&id)
    }

    /// Returns the consumer with this `id`. Returns `None` if this `id` has already been taken.
    #[must_use = "Disruptor will stall if any handle is not used"]
    pub fn take_consumer(&mut self, id: u64) -> Option<Consumer<E, W>> {
        self.consumers.remove(&id)
    }

    /// Returns an iterator of all producers. Returns an empty iterator on all subsequent calls.
    #[must_use = "Disruptor will stall if any handle is not used"]
    pub fn drain_producers(&mut self) -> impl Iterator<Item = (u64, Producer<E, W, false>)> + '_ {
        self.producers.drain()
    }

    /// Returns an iterator of all consumers. Returns an empty iterator on all subsequent calls.
    #[must_use = "Disruptor will stall if any handle is not used"]
    pub fn drain_consumers(&mut self) -> impl Iterator<Item = (u64, Consumer<E, W>)> + '_ {
        self.consumers.drain()
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
        let result = DisruptorBuilder::new(64, || 0)
            .extend_handles([
                (0, Handle::Consumer, Follows::LeadProducer),
                (1, Handle::Consumer, Follows::Handles(vec![0, 3])),
                (2, Handle::Consumer, Follows::Handles(vec![0])),
                (3, Handle::Consumer, Follows::Handles(vec![1])),
                (4, Handle::Consumer, Follows::Handles(vec![2])),
            ])
            .build();
        assert_eq!(result.err().unwrap(), BuildError::GraphCycle(1));

        let result = DisruptorBuilder::new(64, || 0)
            .extend_handles([
                (0, Handle::Consumer, Follows::LeadProducer),
                (1, Handle::Consumer, Follows::Handles(vec![0])),
                (2, Handle::Consumer, Follows::Handles(vec![1, 4])),
                (3, Handle::Consumer, Follows::Handles(vec![2])),
                (4, Handle::Consumer, Follows::Handles(vec![2])),
            ])
            .build();
        assert_eq!(result.err().unwrap(), BuildError::GraphCycle(2));
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
            Err(BuildError::DisconnectedNode(4))
        );
    }

    #[test]
    fn test_find_unregistered_id() {
        let graph = U64Map::from_iter([(0, vec![1])]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(
            validate_graph(&graph, &roots),
            Err(BuildError::UnregisteredID(1))
        );

        let graph = U64Map::from_iter([(0, vec![1, 2]), (1, vec![3]), (2, vec![3])]);
        assert_eq!(
            validate_graph(&graph, &roots),
            Err(BuildError::UnregisteredID(3))
        );
    }

    #[test]
    fn test_find_cycle_self_follow() {
        let graph = U64Map::from_iter([(0, vec![1]), (1, vec![1])]);
        let roots = U64Set::from_iter([0]);
        assert_eq!(
            validate_graph(&graph, &roots),
            Err(BuildError::GraphCycle(1))
        );
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
        assert_eq!(
            result,
            Err(BuildError::UnorderedProducer(vec![0, 1, 2, 3, 7], 4))
        )
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
        assert_eq!(result, Ok(()))
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
        assert_eq!(result, Ok(()))
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
        assert_eq!(result, Ok(()))
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
        assert_eq!(result, Err(BuildError::UnorderedProducer(vec![0, 5], 4)))
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
        assert_eq!(result, Ok(()))
    }

    #[test]
    fn test_builder_empty_follows_disconnected_err() {
        let result = DisruptorBuilder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![]))
            .build();

        assert_eq!(result.err().unwrap(), BuildError::DisconnectedNode(1))
    }

    #[test]
    fn test_builder_buffer_size_error() {
        // buffer size cannot be 0
        let result = DisruptorBuilder::new(0, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .build();
        assert_eq!(result.err().unwrap(), BuildError::BufferSize(0));

        // buffer size must be power of 2
        let result2 = DisruptorBuilder::new(12, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .build();
        assert_eq!(result2.err().unwrap(), BuildError::BufferSize(12))
    }

    #[test]
    fn test_builder_overlapping_ids() {
        let result = DisruptorBuilder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
            .build();

        assert_eq!(result.err().unwrap(), BuildError::OverlappingIDs(vec![1]))
    }

    #[test]
    fn test_builder_empty_graph() {
        let result = DisruptorBuilder::new(32, || 0).build();
        assert_eq!(result.err().unwrap(), BuildError::EmptyGraph)
    }

    #[test]
    fn test_builder_lead_not_followed() {
        let result = DisruptorBuilder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::Handles(vec![8]))
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
            .build();
        assert_eq!(result.err().unwrap(), BuildError::UnregisteredID(8))
    }
}
