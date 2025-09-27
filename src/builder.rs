use crate::handles::{Barrier, Consumer, Cursor, HandleInner, Producer};
use crate::ringbuffer::RingBuffer;
use crate::wait::{WaitPhased, WaitSleep};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// Configures and builds a disruptor.
///
/// Allows users to create custom handle graphs, including trailing producers and concurrent
/// consumers.
///
/// Defaults to a [`WaitPhased<WaitSleep>`](WaitPhased) strategy which busy-spins for 1
/// millisecond, then spin-yields the thread for 1 millisecond, and finally spin-sleeps in 50
/// microsecond increments.
///
/// The configuration of the disruptor is only evaluated when [`build`](Builder::build)
/// is called.
#[derive(Debug)]
pub struct Builder<F, E, W> {
    /// Size of the ring buffer, must be power of 2.
    buffer_size: usize,
    /// Factory function for populating the buffer with events. Wrapped in an option only to
    /// facilitate moving the value out of the builder while the builder is still in use.
    event_factory: Option<F>,
    /// Directly provided storage for the `RingBuffer`.
    provided_buffer: Option<Box<[E]>>,
    /// Factory function for building instances of the wait strategy, defaults to `WaitSleep`.
    wait_strategy: W,
    /// Maps ids of consumers in a "followed by" relationship. For example, the pair  `(3, [5, 7])`
    /// indicates that the consumer with id `3` is followed by the consumers with ids `5` and `7`.
    followed_by: U64Map<Vec<u64>>,
    /// The inverse of `followed_by`, useful for conveniently constructing barriers for handles.
    follows: U64Map<Follows>,
    /// All the ids which follow the lead producer. These are the roots of the graph.
    follows_lead: U64Set,
    /// Maps ids to handle type (producer or consumer).
    handles_map: U64Map<Handle>,
    /// Tracks whether ids have been reused.
    overlapping_ids: U64Set,
}

pub(crate) const BACKOFF_WAIT: WaitPhased<WaitSleep> = WaitPhased::new(
    Duration::from_millis(1),
    Duration::from_millis(1),
    WaitSleep::new(Duration::from_micros(50)),
);

impl<E> Builder<fn() -> E, E, WaitPhased<WaitSleep>> {
    /// Returns a new [`Builder`].
    ///
    /// The size of `buffer` should be a non-zero power of 2, or the call to
    /// [`build`](Builder::build) will fail.
    ///
    /// # Examples
    /// ```
    /// use ansa::Builder;
    ///
    /// let buffer = vec![0u8; 1024];
    ///
    /// let builder = Builder::with_buffer(buffer.into_boxed_slice());
    /// ```
    pub fn with_buffer(buffer: Box<[E]>) -> Self
    where
        E: Sync,
    {
        Builder {
            buffer_size: buffer.len(),
            event_factory: None,
            provided_buffer: Some(buffer),
            wait_strategy: BACKOFF_WAIT,
            followed_by: U64Map::default(),
            follows: U64Map::default(),
            follows_lead: U64Set::default(),
            handles_map: U64Map::default(),
            overlapping_ids: U64Set::default(),
        }
    }
}

impl<F, E> Builder<F, E, WaitPhased<WaitSleep>> {
    /// Returns a new [`Builder`].
    ///
    /// `event_factory` is used to populate the buffer.
    ///
    /// `size` must be a non-zero power of 2, or the call to [`build`](Builder::build) will fail.
    ///
    /// # Examples
    ///
    /// Using `i64` as the event type:
    /// ```
    /// use ansa::Builder;
    ///
    /// // with a very simple event_factory
    /// let builder = Builder::new(64, || 0i64);
    ///
    /// // using a closure to capture state
    /// let mut counter = -1_i64;
    /// let factory = move || {
    ///     counter += 1;
    ///     counter
    /// };
    ///
    /// let builder = Builder::new(64, factory);
    /// ```
    /// With a complex event type:
    /// ```
    /// use ansa::Builder;
    ///
    /// #[derive(Default)]
    /// enum Currency {
    ///     #[default]
    ///     None, // indicates event initialised but not yet containing real data
    ///     GBP,
    ///     USD,
    ///     // ... all the rest
    /// }
    ///
    /// #[derive(Default)]
    /// struct Event {
    ///     price: u64,
    ///     time: u64,
    ///     currency: Currency,
    /// }
    ///
    /// let builder = Builder::new(256, Event::default);
    /// ```
    pub fn new(size: usize, event_factory: F) -> Self
    where
        E: Sync,
        F: FnMut() -> E,
    {
        Builder {
            buffer_size: size,
            event_factory: Some(event_factory),
            provided_buffer: None,
            wait_strategy: BACKOFF_WAIT,
            followed_by: U64Map::default(),
            follows: U64Map::default(),
            follows_lead: U64Set::default(),
            handles_map: U64Map::default(),
            overlapping_ids: U64Set::default(),
        }
    }
}

impl<F, E, W> Builder<F, E, W>
where
    F: FnMut() -> E,
    W: Clone,
{
    /// Add a handle to the disruptor.
    ///
    /// Each handle must be associated with a unique `id`, and must be either a [`Consumer`] or
    /// [`Producer`].
    ///
    /// In the disruptor pattern, management of ring buffer accesses works by ordering handles
    /// according to a precedence, or "follows", relationship. Every handle must describe how it
    /// relates to other handles by indicating which handles it follows.
    ///
    /// If handle `B` follows handle `A`, then `B` accesses events on the ring buffer exclusively
    /// after `A`. See: [`Follows`] for more.
    ///
    /// These precedence relationships must form a [directed acyclic graph][dag] (no loops allowed),
    /// where handles follow the lead producer or other handles. Furthermore, every handle must
    /// follow or be followed by every producer. See below for more details about ordering handles.
    ///
    /// [dag]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
    ///
    /// The "lead producer" is automatically defined for every disruptor. It is always a producer,
    /// and it serves as the root of the graph. Every other handle must follow the lead either
    /// directly or indirectly.
    ///
    /// # Ordering Handles
    ///
    /// ## Consumers
    ///
    /// Consumers can follow each other.
    /// ```
    /// use ansa::{Builder, Follows, Handle};
    ///
    /// // lead ◄─ 0 ◄─ 1
    /// let _ = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .build()?;
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    /// But consumer handles do not require exclusive access to the buffer, which allows for
    /// multiple consumers to read the ring buffer concurrently.
    /// ```
    /// use ansa::{Builder, Follows, Handle};
    ///
    /// // lead ◄─ 0
    /// //    ▲
    /// //    │
    /// //    1
    /// let _ = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::LeadProducer)
    ///     .build()?;
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    /// Both consumers `0` and `1` will read all the events published by the lead producer, without
    /// coordinating with each other.
    ///
    /// ## Producers
    ///
    /// Producer handles must always either follow or be followed by every other handle.
    ///
    /// In more technical terms: it is possible for consumer handles to be partially ordered with
    /// respect to other consumer handles, but all handles (of any kind) must be totally ordered
    /// with respect to all producer handles.
    ///
    /// If this restriction was not in place, overlapping writes to the buffer (read: mutable
    /// aliasing and so undefined behaviour) would be possible.
    ///
    /// For examples of invalid producer ordering, see: [`BuildError::UnorderedProducer`].
    ///
    /// Below is an example of a validly ordered producer in a graph.
    /// ```
    /// use ansa::{Builder, Follows, Handle};
    ///
    /// // lead ◄─ 0 ◄─ 2
    /// //         ▲    ▲
    /// //         │    │
    /// //         1 ◄─ 3P ◄─ 4
    /// let _ = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .add_handle(2, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .add_handle(3, Handle::Producer, Follows::Handles(vec![1, 2]))
    ///     .add_handle(4, Handle::Consumer, Follows::Handles(vec![3]))
    ///     .build()?;
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    /// See: [`MultiProducer`](crate::handles::MultiProducer) for one means to parallelize writes.
    ///
    /// See: [`BuildError`] for full details on error states encountered when building the
    /// disruptor. Such errors are likely to be caused by calls to this function or
    /// [`extend_handles`](Builder::extend_handles) creating an invalid graph.
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
            Follows::Handles(ids) => ids.iter().for_each(|follow_id| {
                self.followed_by
                    .entry(*follow_id)
                    .and_modify(|vec| vec.push(id))
                    .or_insert_with(|| vec![id]);
            }),
        }
        self.follows.insert(id, follows);
        self
    }

    /// Extend the handles graph with an iterator.
    ///
    /// The implementation simply calls [`add_handle`](Builder::add_handle) on each
    /// element of the iterator.
    ///
    /// # Examples
    ///
    ///```
    /// use ansa::{Builder, Follows, Handle};
    ///
    /// let _ = Builder::new(64, || 0)
    ///    .extend_handles([
    ///        (0, Handle::Consumer, Follows::LeadProducer),
    ///        (1, Handle::Consumer, Follows::LeadProducer),
    ///        (2, Handle::Consumer, Follows::Handles(vec![0, 1]))
    ///    ])
    ///    .build()?;
    /// # Ok::<(), ansa::BuildError>(())
    ///```
    pub fn extend_handles(self, iter: impl IntoIterator<Item = (u64, Handle, Follows)>) -> Self {
        let mut this = self;
        for (id, handle, follows) in iter {
            this = this.add_handle(id, handle, follows);
        }
        this
    }

    /// Set the wait strategy to be used by all consumers and producers.
    ///
    /// For details of all provided wait strategies, see the module docs for [`wait`](crate::wait).
    ///
    /// # Examples
    ///
    /// ```
    /// use ansa::{Builder, Follows, Handle};
    /// use ansa::wait::WaitSleep;
    /// use std::time::Duration;
    ///
    /// let _ = Builder::new(32, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .wait_strategy(WaitSleep::new(Duration::from_nanos(500)))
    ///     .build()?;
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    pub fn wait_strategy<W2>(self, strategy: W2) -> Builder<F, E, W2>
    where
        W2: Clone,
    {
        Builder {
            buffer_size: self.buffer_size,
            event_factory: self.event_factory,
            provided_buffer: None,
            wait_strategy: strategy,
            followed_by: self.followed_by,
            follows: self.follows,
            follows_lead: self.follows_lead,
            handles_map: self.handles_map,
            overlapping_ids: self.overlapping_ids,
        }
    }

    /// Returns the constructed [`Disruptor`] if successful, otherwise returns [`BuildError`].
    ///
    /// For full details on error states, please see [`BuildError`].
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Producer};
    ///
    /// let _ = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()?;
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    #[track_caller]
    pub fn build(mut self) -> Result<Disruptor<E, W>, BuildError> {
        self.validate()?;
        let buffer = Arc::new(self.construct_buffer());
        let lead_cursor = Arc::new(Cursor::start());
        let (producers, consumers, cursor_map) = self.construct_handles(&lead_cursor, &buffer);
        let barrier = self.construct_lead_barrier(cursor_map);
        let lead = HandleInner::new(lead_cursor, barrier, buffer, self.wait_strategy.clone());

        Ok(Disruptor {
            lead: Some(lead.into_producer()),
            producers,
            consumers,

            #[cfg(debug_assertions)]
            build_position: {
                use std::panic::Location;
                let location = Location::caller();
                format!(
                    "{}:{}:{}",
                    location.file(),
                    location.line(),
                    location.column()
                )
            },
        })
    }

    fn construct_buffer(&mut self) -> RingBuffer<E> {
        match (self.event_factory.take(), self.provided_buffer.take()) {
            (Some(factory), None) => RingBuffer::from_factory(self.buffer_size, factory),
            (None, Some(buffer)) => RingBuffer::from_buffer(buffer),
            _ => unreachable!("guaranteed by Builder construction methods"),
        }
    }

    fn construct_lead_barrier(&self, cursor_map: U64Map<Arc<Cursor>>) -> Barrier {
        let mut tail_cursors: Vec<_> = self
            .followed_by
            .iter()
            .filter(|(_, followed_by)| followed_by.is_empty())
            // unwrap okay as the cursor for this id must exist by this point
            .map(|(id, _)| Arc::clone(cursor_map.get(id).unwrap()))
            .collect();

        debug_assert!(!tail_cursors.is_empty());
        match tail_cursors.len() {
            1 => Barrier::one(tail_cursors.pop().unwrap()),
            _ => Barrier::many(tail_cursors.into_boxed_slice()),
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
            let cursor = map.entry(id).or_insert_with(|| Arc::new(Cursor::start()));
            Arc::clone(cursor)
        }

        for (&id, follows) in &self.follows {
            let cursor = get_cursor(id, &mut cursor_map);
            let buf = Arc::clone(buffer);
            let barrier = match follows {
                Follows::LeadProducer => Barrier::one(Arc::clone(lead_cursor)),
                Follows::Handles(ids) if ids.len() == 1 => {
                    Barrier::one(get_cursor(ids[0], &mut cursor_map))
                }
                // todo: randomise order of cursors in barrier::many, to help ease contention
                //  between multiple consumers following the same set of cursors.
                Follows::Handles(ids) => {
                    let follows_cursors = ids
                        .iter()
                        .map(|follow_id| get_cursor(*follow_id, &mut cursor_map))
                        .collect();
                    Barrier::many(follows_cursors)
                }
            };

            let handle = HandleInner::new(cursor, barrier, buf, self.wait_strategy.clone());

            // unwrap okay as this entry in handles_map is guaranteed to exist for this id
            match self.handles_map.get(&id).unwrap() {
                Handle::Producer => {
                    producers.insert(id, handle.into_producer());
                }
                Handle::Consumer => {
                    consumers.insert(id, handle.into_consumer());
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
        if self.buffer_size == 0 || !self.buffer_size.is_power_of_two() {
            return Err(BuildError::BufferSize(self.buffer_size));
        }
        if !self.overlapping_ids.is_empty() {
            return Err(BuildError::OverlappingIDs(
                self.overlapping_ids.iter().copied().collect(),
            ));
        }
        let chains = validate_graph(&self.followed_by, &self.follows_lead)?;
        validate_order(&self.handles_map, chains)?;
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
    /// Indicates that these handle ids are not ordered with respect to this producer id. Unordered
    /// producers are prohibited, as they may overlap mutable buffer accesses with other handles
    /// running in other processes, potentially causing Undefined behaviour due to mutable aliasing.
    ///
    /// ## Example Causes
    /// ```
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// // lead ◄─ 1 ◄─ 2P
    /// //    ▲
    /// //    │
    /// //    0
    /// let result = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(2, Handle::Producer, Follows::Handles(vec![1]))
    ///     .build();
    ///
    /// assert_eq!(result.unwrap_err(), BuildError::UnorderedProducer(vec![0], 2));
    /// ```
    /// Since producer `2` does not explicitly follow consumer `1`, it cannot be guaranteed that
    /// their buffer access do not overlap. We can fix this by adding id `1` to the `Follows` vec
    /// for producer `2`.
    ///
    /// ```
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// // lead ◄─ 0 ◄─ 2 ◄─ 3 ◄─ 4 ◄─ 5P
    /// //         ▲
    /// //         │
    /// //         1
    /// let result = Builder::new(64, || 0)
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
    /// assert_eq!(result.unwrap_err(), BuildError::UnorderedProducer(vec![0, 1], 5));
    /// ```
    /// Even though it may appear very likely that consumer `1` will finish before producer `5`, it
    /// cannot be guaranteed. This can be fixed by adding id `1` to the `Follows` vec for producer
    /// `5`.
    ///
    /// ```
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// // lead ◄─ 0 ◄─ 1P ◄─ 2 ◄─ 3 ◄─ 7
    /// //         ▲                    │
    /// //         │                    │
    /// //         4 ◄─ 5 ◄─ 6 ◄────────┘
    /// let result = Builder::new(64, || 0)
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
    /// assert_eq!(result.unwrap_err(), BuildError::UnorderedProducer(vec![0, 4, 5, 6, 7], 1));
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
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// let result = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0, 2])) // <- here
    ///     .build();
    ///
    /// assert_eq!(result.unwrap_err(), BuildError::UnregisteredID(2));
    /// ```
    UnregisteredID(u64),
    /// An ID which loops in the graph. Loops between handles will cause the disruptor to deadlock.
    ///
    /// ## Example Cause
    /// ```
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// // lead ◄─ 0 ◄─ 1 ◄─ 2
    /// //              │    ▲
    /// //              └────┘
    /// let result = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0, 2])) // <- here
    ///     .add_handle(2, Handle::Consumer, Follows::Handles(vec![1]))
    ///     .build();
    ///
    /// assert_eq!(result.unwrap_err(), BuildError::GraphCycle(1));
    /// ```
    GraphCycle(u64),
    /// An ID which is disconnected from the rest of the graph. Disconnected in this context means
    /// this id is not reachable from the root of the graph when traversing the follows relationships.
    ///
    /// ## Example Cause
    /// ```
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// let result = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![])) // <- here
    ///     .build();
    ///
    /// assert_eq!(result.unwrap_err(), BuildError::DisconnectedNode(1));
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
        write!(f, "{str}")
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
        let visited_chains = visit(*node, &mut visiting, &mut visited, Vec::new(), graph)?;
        chains.extend(visited_chains);
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
/// <https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search>
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
        chains.push(chain);
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
/// `(chain, id)` pair where the producer `id` is not in the `chain`.
///
/// A chain is a totally-ordered subset of a partially-ordered set (poset). If a chain does not
/// include an element, `e`, of the poset, then there exist elements of that chain which are
/// unordered with respect to `e`. Thus, to ensure that every element in the graph is totally
/// ordered with respect to all producers, it must hold that all chains contain all producers.
///
/// Another way of thinking about this is: it should not be possible to traverse the DAG from the
/// root to any single leaf node without visiting every producer.
fn validate_order(handles_map: &U64Map<Handle>, chains: Vec<Vec<u64>>) -> Result<(), BuildError> {
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
                return Err(BuildError::UnorderedProducer(chain, *id));
            }
        }
    }
    Ok(())
}

/// Describes the ordering relationship for a single handle.
///
/// # Examples
/// ```
/// use ansa::Follows;
///
/// // ordered directly after the lead producer
/// let _ = Follows::LeadProducer;
///
/// // ordered directly after the handle with id `0`
/// let _ = Follows::Handles(vec![0]);
///
/// // ordered directly after the handles with ids `0`, `1` and `2`
/// let _ = Follows::Handles(vec![0, 1, 2]);
/// ```
/// When one handle is 'ordered' after another, it will interact with a sequence on the buffer only
/// after all handles it follows have concluded their interactions with that sequence.
///
/// In disruptor terms: a handle's barrier includes the cursors for all handles it directly follows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Follows {
    /// Following the lead producer, which has no id.
    LeadProducer,
    /// The list of ids of handles to follow
    Handles(Vec<u64>),
}

/// Describes the type of handle being added to the disruptor.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Handle {
    /// A handle which can write to the buffer.
    Producer,
    /// A read-only handle.
    Consumer,
}

/// Holds the producers and consumers for a single disruptor.
///
/// **Warning**: It is likely a programming error to leave unused any producers and consumers held
/// by this object. If any single producer or consumer fails to move on the ring buffer, then the
/// disruptor as a whole will eventually permanently stall.
///
/// Once empty, this object can be freely dropped.
///
/// When `debug_assertions` are enabled, a warning is printed if this object is not empty when
/// dropped, i.e., there are still further producers or consumers to extract.
#[derive(Debug)]
pub struct Disruptor<E, W> {
    lead: Option<Producer<E, W, true>>,
    producers: U64Map<Producer<E, W, false>>,
    consumers: U64Map<Consumer<E, W>>,
    #[cfg(debug_assertions)]
    build_position: String,
}

impl<E, W> Disruptor<E, W> {
    /// Returns the lead producer when first called, and returns `None` on all subsequent calls.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Producer};
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()?;
    ///
    /// let lead = disruptor.take_lead();
    /// assert!(matches!(lead, Some(Producer { .. })));
    ///
    /// let take_again = disruptor.take_lead();
    /// assert!(take_again.is_none());
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    #[must_use = "Disruptor will stall if any handle is left unused"]
    pub fn take_lead(&mut self) -> Option<Producer<E, W, true>> {
        self.lead.take()
    }

    /// Returns the producer with this `id`, or `None` if this `id` has already been taken.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Producer};
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
    ///     .build()?;
    ///
    /// let producer = disruptor.take_producer(0);
    /// assert!(matches!(producer, Some(Producer { .. })));
    ///
    /// let take_again = disruptor.take_producer(0);
    /// assert!(take_again.is_none());
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    #[must_use = "Disruptor will stall if any handle is left unused"]
    pub fn take_producer(&mut self, id: u64) -> Option<Producer<E, W, false>> {
        self.producers.remove(&id)
    }

    /// Returns the consumer with this `id`, or `None` if this `id` has already been taken.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Consumer};
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()?;
    ///
    /// let consumer = disruptor.take_consumer(0);
    /// assert!(matches!(consumer, Some(Consumer { .. })));
    ///
    /// let take_again = disruptor.take_consumer(0);
    /// assert!(take_again.is_none());
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    #[must_use = "Disruptor will stall if any handle is left unused"]
    pub fn take_consumer(&mut self, id: u64) -> Option<Consumer<E, W>> {
        self.consumers.remove(&id)
    }

    /// Returns an iterator of all producers. Returns an empty iterator on all subsequent calls.
    ///
    /// **Warning**: If the returned iterator is dropped before being fully consumed, it drops the
    /// remaining (id, producer) pairs. Dropped handles will cause the disruptor to stall.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Producer};
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Producer, Follows::Handles(vec![0]))
    ///     .build()?;
    ///
    /// let producers: Vec<_> = disruptor.drain_producers().collect();
    ///
    /// let (id_0, _) = &producers[0];
    /// assert_eq!(*id_0, 0);
    ///
    /// let (id_1, _) = &producers[1];
    /// assert_eq!(*id_1, 1);
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    #[must_use = "Disruptor will stall if any handle is left unused"]
    pub fn drain_producers(&mut self) -> impl Iterator<Item = (u64, Producer<E, W, false>)> + '_ {
        self.producers.drain()
    }

    /// Returns an iterator of all consumers. Returns an empty iterator on all subsequent calls.
    ///
    /// **Warning**: If the returned iterator is dropped before being fully consumed, it drops the
    /// remaining (id, consumer) pairs. Dropped handles will cause the disruptor to stall.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Producer};
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::LeadProducer)
    ///     .build()?;
    ///
    /// let consumers: Vec<_> = disruptor.drain_consumers().collect();
    ///
    /// let (id_0, _) = &consumers[0];
    /// assert_eq!(*id_0, 0);
    ///
    /// let (id_1, _) = &consumers[1];
    /// assert_eq!(*id_1, 1);
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    #[must_use = "Disruptor will stall if any handle is left unused"]
    pub fn drain_consumers(&mut self) -> impl Iterator<Item = (u64, Consumer<E, W>)> + '_ {
        self.consumers.drain()
    }

    /// Returns `true` when no handles are left to take from this object.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Builder, Follows, Handle, Producer};
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
    ///     .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .add_handle(2, Handle::Consumer, Follows::Handles(vec![0]))
    ///     .build()?;
    ///
    /// let _ = disruptor.take_lead();
    /// let _ = disruptor.drain_producers();
    /// let _ = disruptor.drain_consumers();
    ///
    /// assert_eq!(disruptor.is_empty(), true);
    /// # Ok::<(), ansa::BuildError>(())
    /// ```
    pub fn is_empty(&self) -> bool {
        self.lead.is_none() && self.consumers.is_empty() && self.producers.is_empty()
    }
}

#[cfg(debug_assertions)]
impl<E, W> Drop for Disruptor<E, W> {
    fn drop(&mut self) {
        if !self.is_empty() {
            eprintln!("warning: Disruptor not empty when dropped, build location:");
            eprintln!("   --> {}", self.build_position);
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
        let result = Builder::new(64, || 0)
            .extend_handles([
                (0, Handle::Consumer, Follows::LeadProducer),
                (1, Handle::Consumer, Follows::Handles(vec![0, 3])),
                (2, Handle::Consumer, Follows::Handles(vec![0])),
                (3, Handle::Consumer, Follows::Handles(vec![1])),
                (4, Handle::Consumer, Follows::Handles(vec![2])),
            ])
            .build();
        assert_eq!(result.err().unwrap(), BuildError::GraphCycle(1));

        let result = Builder::new(64, || 0)
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

    // 0 ◄─ 1 ◄─ 2 ◄─ 3 ◄─ 7
    // ▲              │
    // │              │
    // 4 ◄─ 5 ◄─ 6 ◄──┘
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

    // 0  ◄─ 1 ◄─ 2 ◄─ 3 ◄─ 7
    // ▲               │
    // │               │
    // 4P ◄─ 5 ◄─ 6 ◄──┘
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
        let result = validate_order(&handles_map, chains);
        assert_eq!(
            result,
            Err(BuildError::UnorderedProducer(vec![0, 1, 2, 3, 7], 4))
        )
    }

    // 0 ◄─ 1 ◄─ 2 ◄─ 3P ◄─ 7
    // ▲              │
    // │              │
    // 4 ◄─ 5 ◄─ 6 ◄──┘
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
        let result = validate_order(&handles_map, chains);
        assert_eq!(result, Ok(()))
    }

    // 0P ◄─ 1 ◄─ 2 ◄─ 3 ◄─ 7
    // ▲               │
    // │               │
    // 4  ◄─ 5 ◄─ 6 ◄──┘
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
        let result = validate_order(&handles_map, chains);
        assert_eq!(result, Ok(()))
    }

    // 0 ◄─ 1
    // ▲    ▲
    // │    │
    // 2 ◄─ 3P ◄─ 4
    //      ▲     ▲
    //      │     │
    //      5  ◄─ 6P
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
        let result = validate_order(&handles_map, chains);
        assert_eq!(result, Ok(()))
    }

    // 0 ◄─ 1 ◄─ 2 ◄─ 3 ◄─ 4P
    // ▲
    // │
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
        let result = validate_order(&handles_map, chains);
        assert_eq!(result, Err(BuildError::UnorderedProducer(vec![0, 5], 4)))
    }

    // 0 ◄─ 1 ◄─ 2 ◄─ 3 ◄─ 4
    // ▲
    // │
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
        let result = validate_order(&handles_map, chains);
        assert_eq!(result, Ok(()))
    }

    // Lead ◄─ 1P
    //    ▲
    //    │
    //    0
    #[test]
    fn test_producer_total_order5() {
        let build_err = Builder::new(64, || 0i64)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Producer, Follows::LeadProducer)
            .validate()
            .unwrap_err();

        assert_eq!(build_err, BuildError::UnorderedProducer(vec![0], 1))
    }

    #[test]
    fn test_builder_empty_follows_disconnected_err() {
        let result = Builder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![]))
            .build();

        assert_eq!(result.err().unwrap(), BuildError::DisconnectedNode(1))
    }

    #[test]
    fn test_builder_buffer_size_error() {
        // buffer size cannot be 0
        let result = Builder::new(0, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .build();
        assert_eq!(result.err().unwrap(), BuildError::BufferSize(0));

        // buffer size must be power of 2
        let result2 = Builder::new(12, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .build();
        assert_eq!(result2.err().unwrap(), BuildError::BufferSize(12))
    }

    #[test]
    fn test_builder_overlapping_ids() {
        let result = Builder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
            .build();

        assert_eq!(result.err().unwrap(), BuildError::OverlappingIDs(vec![1]))
    }

    #[test]
    fn test_builder_empty_graph() {
        let result = Builder::new(32, || 0).build();
        assert_eq!(result.err().unwrap(), BuildError::EmptyGraph)
    }

    #[test]
    fn test_builder_lead_not_followed() {
        let result = Builder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::Handles(vec![8]))
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
            .build();
        assert_eq!(result.err().unwrap(), BuildError::UnregisteredID(8));
    }

    #[test]
    fn test_build_ok() {
        let result = Builder::new(32, || 0)
            .add_handle(0, Handle::Consumer, Follows::LeadProducer)
            .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
            .build();
        assert!(result.is_ok());
        let mut d = result.unwrap();
        let _ = d.take_lead();
        let _ = d.drain_consumers();
        // no warning for non-empty disruptor should be printed
    }
}
