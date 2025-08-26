use crate::ringbuffer::RingBuffer;
use crate::wait::{TryWaitStrategy, WaitStrategy};
use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::{fence, AtomicI64, Ordering};
use std::sync::Arc;

/// A handle with mutable access to events on the ring buffer.
///
/// `MultiProducer`s can be cloned to enable distributed writes. Clones coordinate by claiming
/// non-overlapping ranges of sequence values, which can be writen to in parallel.
///
/// Clones of this `MultiProducer` share this producer's cursor.
///
/// See: **\[INSERT LINK]** for alternative methods of parallelizing producers without using
/// `MultiProducer`.
///
/// # Examples
/// ```
/// use ansa::{Builder, Follows, Handle};
///
/// let mut disruptor = Builder::new(64, || 0)
///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
///     .build()?;
///
/// // lead and trailing are separate handles with separate cursors and clones
/// let lead = disruptor.take_lead().unwrap().into_multi();
/// let trailing = disruptor.take_producer(0).unwrap().into_multi();
///
/// let lead_clone = lead.clone();
///
/// assert_eq!(lead.count(), 2);
/// assert_eq!(trailing.count(), 1);
/// # Ok::<(), ansa::BuildError>(())
/// ```
///
/// # Limitations
///
/// The `MultiProducer` API is quite limited in comparison to either [`Producers`](Producer) or
/// [`Consumers`](Consumer). This is due to the mechanism used to coordinate `MultiProducer` clones.
///
/// TL;DR, the `MultiProducer` limitations are:
/// 1) `wait_range` method cannot be implemented.
/// 2) Separate `wait` and `apply` methods cannot be conveniently provided.
/// 3) The `try_wait_apply` method will always move the cursor up by the requested batch size,
///    even if an error occurs.
///
/// As soon as a `MultiProducer` clone claims and waits for available sequences, it is commited to
/// moving the shared cursor up to the end of the claimed batch, regardless of whether either
/// waiting for or processing the batch is successful or not.
///
/// This commitment requires that batches always be completed (and the cursor moved), even for
/// fallible methods where errors might occur. Importantly, this means that handles which follow a
/// `MultiProducer` cannot assume, e.g., that an event was successfully written based only on its
/// sequence becoming available.
///
/// Another consequence is that batches cannot be sized dynamically depending on what sequences are
/// actually available, as the bounds of the claim must be used. Hence, `wait_range` cannot be
/// implemented.
///
/// Lastly, separate `wait` methods cannot be provided, because [`EventsMut`] structs cannot be
/// guaranteed to operate correctly in user code. If an `EventsMut` is dropped without an apply
/// method being called, then the claim it was generated from will go unused, ultimately causing
/// permanent disruptor stalls. Instead, combined wait and apply methods are provided.
#[derive(Debug)]
pub struct MultiProducer<E, W, const LEAD: bool> {
    inner: HandleInner<E, W, LEAD>,
    claim: Arc<Cursor>, // shared between all clones of this multi producer
}

impl<E, W, const LEAD: bool> Clone for MultiProducer<E, W, LEAD>
where
    W: Clone,
{
    fn clone(&self) -> Self {
        MultiProducer {
            inner: HandleInner {
                cursor: Arc::clone(&self.inner.cursor),
                barrier: self.inner.barrier.clone(),
                buffer: Arc::clone(&self.inner.buffer),
                wait_strategy: self.inner.wait_strategy.clone(),
                available: self.inner.available,
            },
            claim: Arc::clone(&self.claim),
        }
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD> {
    /// Returns `true` if this producer is the lead producer.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let mut disruptor = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
    ///     .build()?;
    ///
    /// let lead_multi = disruptor.take_lead().unwrap().into_multi();
    /// let follow_multi = disruptor.take_producer(0).unwrap().into_multi();
    ///
    /// assert_eq!(lead_multi.is_lead(), true);
    /// assert_eq!(follow_multi.is_lead(), false);
    /// # Ok::<(), BuildError>(())
    /// ```
    pub const fn is_lead(&self) -> bool {
        LEAD
    }

    /// Returns the count of [`MultiProducer`]s associated with this cursor.
    ///
    /// **Important:** Care should be taken when performing actions based upon this number, as any
    /// thread which holds an associated [`MultiProducer`] may clone it at any time, thereby
    /// changing the count.
    ///
    /// # Examples
    /// ```
    /// let (multi, _) = ansa::mpsc(64, || 0);
    /// assert_eq!(multi.count(), 1);
    ///
    /// let multi_2 = multi.clone();
    /// assert_eq!(multi.count(), 2);
    /// // consume a `MultiProducer` by attempting the conversion into a `Producer`
    /// assert!(matches!(multi.into_producer(), None));
    /// assert_eq!(multi_2.count(), 1);
    /// ```
    #[inline]
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.claim)
    }

    /// Returns the current sequence value for this producer's cursor.
    ///
    /// **Important:** The cursor for a `MultiProducer` is shared by all of its clones. Any of
    /// these clones could alter this value at any time, if they are writing to the buffer.
    ///
    /// # Examples
    /// ```
    /// let (mut multi, _) = ansa::mpsc(64, || 0);
    /// // sequences start at -1, but accesses always occur at the next sequence,
    /// // so the first accessed sequence will be 0
    /// assert_eq!(multi.sequence(), -1);
    ///
    /// // move the producer cursor up by 10
    /// multi.wait_for_each(10, |_, _, _| ());
    /// assert_eq!(multi.sequence(), 9);
    ///
    /// // clone and move only the clone
    /// let mut clone = multi.clone();
    /// clone.wait_for_each(10, |_, _, _| ());
    ///
    /// // Both have moved because all clones share the same cursor
    /// assert_eq!(multi.sequence(), 19);
    /// assert_eq!(clone.sequence(), 19);
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        self.inner.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    ///
    /// # Examples
    /// ```
    /// let (producer, _) = ansa::mpsc(64, || 0);
    /// assert_eq!(producer.buffer_size(), 64);
    /// ```
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.inner.buffer_size()
    }

    /// Set the wait strategy for this `MultiProducer` clone.
    ///
    /// Does not alter the wait strategy for any other handle, _including_ other clones of this
    /// `MultiProducer`.
    ///
    /// # Examples
    /// ```
    /// use ansa::MultiProducer;
    /// use ansa::wait::{WaitBusy, WaitPhased, WaitSleep};
    ///
    /// let (multi, _) = ansa::mpsc(64, || 0u8);
    /// let clone = multi.clone();
    /// // multi changes its strategy
    /// let _: MultiProducer<u8, WaitBusy, true> = multi.set_wait_strategy(WaitBusy);
    /// // clone keeps the original wait strategy
    /// let _: MultiProducer<u8, WaitPhased<WaitSleep>, true> = clone;
    /// ```
    #[inline]
    pub fn set_wait_strategy<W2>(self, wait_strategy: W2) -> MultiProducer<E, W2, LEAD> {
        MultiProducer {
            inner: self.inner.set_wait_strategy(wait_strategy),
            claim: self.claim,
        }
    }

    /// Return a [`Producer`] if exactly one `MultiProducer` exists for this cursor.
    ///
    /// Otherwise, return `None` and drops this producer.
    ///
    /// If this function is called when only one `MultiProducer` exists, then it is guaranteed to
    /// return a `Producer`.
    ///
    /// # Examples
    /// ```
    /// let (multi, _) = ansa::mpsc(64, || 0);
    /// let multi_clone = multi.clone();
    ///
    /// assert!(matches!(multi.into_producer(), None));
    /// assert!(matches!(multi_clone.into_producer(), Some(ansa::Producer { .. })));
    /// ```
    #[inline]
    pub fn into_producer(self) -> Option<Producer<E, W, LEAD>> {
        Arc::into_inner(self.claim).map(|_| self.inner.into_producer())
    }

    #[inline]
    fn wait_bounds(&self, size: i64) -> (i64, i64) {
        let mut current_claim = self.claim.sequence.load(Ordering::Relaxed);
        let mut claim_end = current_claim + size;
        while let Err(new_current) = self.claim.sequence.compare_exchange(
            current_claim,
            claim_end,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            current_claim = new_current;
            claim_end = new_current + size;
        }
        let desired_seq = if LEAD {
            claim_end - to_i64_saturated(self.inner.buffer.size())
        } else {
            claim_end
        };
        (current_claim, desired_seq)
    }

    #[inline]
    fn update_cursor(&self, start: i64, end: i64) {
        while self.inner.cursor.sequence.load(Ordering::Acquire) != start {
            std::hint::spin_loop();
        }
        self.inner.cursor.sequence.store(end, Ordering::Release)
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// Wait for and process a batch of exactly `size` number of events.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    #[inline]
    pub fn wait_for_each<F>(&mut self, size: usize, mut f: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        debug_assert!(size <= self.inner.buffer.size());
        let (from_seq, till_seq) = self.wait_bounds(to_i64_saturated(size));
        if self.inner.available < till_seq {
            self.inner.available = self.inner.wait_strategy.wait(till_seq, &self.inner.barrier);
        }
        debug_assert!(self.inner.available >= till_seq);
        fence(Ordering::Acquire);
        // SAFETY: Acquire-Release barrier ensures following handles cannot access this sequence
        // before this handle has finished interacting with it. Construction of the disruptor
        // guarantees producers don't overlap with other handles, thus no mutable aliasing.
        unsafe {
            self.inner.buffer.apply(from_seq + 1, size, |ptr, seq, end| {
                // SAFETY: deref guaranteed safe by ringbuffer initialisation
                let event: &mut E = &mut *ptr;
                f(event, seq, end)
            })
        };
        // Ordering::Release performed by this method
        self.update_cursor(from_seq, from_seq + to_i64_saturated(size))
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// Wait for and process a batch of exactly `size` number of events.
    ///
    /// If waiting fails, returns the wait error, `W::Error`, which must be convertible to `Err`.
    ///
    /// If processing the batch fails, returns `Err`.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    #[inline]
    pub fn try_wait_for_each<F, Err>(&mut self, size: usize, mut f: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
        Err: From<W::Error>,
    {
        debug_assert!(size <= self.inner.buffer.size());
        let (from_seq, till_seq) = self.wait_bounds(to_i64_saturated(size));
        fence(Ordering::Acquire);
        if self.inner.available < till_seq {
            self.inner.available =
                self.inner.wait_strategy.try_wait(till_seq, &self.inner.barrier).inspect_err(
                    |_| self.update_cursor(from_seq, from_seq + to_i64_saturated(size)),
                )?;
        }
        debug_assert!(self.inner.available >= till_seq);
        // SAFETY: Acquire-Release barrier ensures following handles cannot access this sequence
        // before this handle has finished interacting with it. Construction of the disruptor
        // guarantees producers don't overlap with other handles, thus no mutable aliasing.
        let result = unsafe {
            self.inner.buffer.try_apply(from_seq + 1, size, |ptr, seq, end| {
                // SAFETY: deref guaranteed safe by ringbuffer initialisation
                let event: &mut E = &mut *ptr;
                f(event, seq, end)
            })
        };
        // Ordering::Release performed by this method
        self.update_cursor(from_seq, from_seq + to_i64_saturated(size));
        result
    }
}

/// A handle with mutable access to events on the ring buffer.
///
/// _Cannot_ access events concurrently with other handles.
#[derive(Debug)]
#[repr(transparent)]
pub struct Producer<E, W, const LEAD: bool> {
    inner: HandleInner<E, W, LEAD>,
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD> {
    /// Returns `true` if this producer is the lead producer, and `false` otherwise.
    ///
    /// # Examples
    /// ```
    /// use ansa::{BuildError, Builder, Follows, Handle};
    ///
    /// let mut builder = Builder::new(64, || 0)
    ///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
    ///     .build()?;
    ///
    /// let lead = builder.take_lead().unwrap();
    /// assert_eq!(lead.is_lead(), true);
    ///
    /// let follows = builder.take_producer(0).unwrap();
    /// assert_eq!(follows.is_lead(), false);
    ///
    /// # Ok::<(), BuildError>(())
    /// ```
    pub const fn is_lead(&self) -> bool {
        LEAD
    }

    /// Returns the current sequence value for this producer's cursor.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    /// // sequences start at -1, but accesses always occur at the next sequence,
    /// // so the first accessed sequence will be 0
    /// assert_eq!(producer.sequence(), -1);
    ///
    /// // move the producer up by 10
    /// producer.wait(10).consume();
    /// assert_eq!(producer.sequence(), 9);
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        self.inner.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    ///
    /// # Examples
    /// ```
    /// let (producer, _) = ansa::spsc(64, || 0);
    /// assert_eq!(producer.buffer_size(), 64);
    /// ```
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.inner.buffer_size()
    }

    /// Returns a new handle with the given `wait_strategy`, consuming this handle.
    ///
    /// All other properties of the original handle remain unchanged in the new handle.
    ///
    /// Does not alter the wait strategy for any other handle.
    ///
    /// # Examples
    /// ```
    /// use ansa::Producer;
    /// use ansa::wait::WaitBusy;
    ///
    /// let (producer, _) = ansa::spsc(64, || 0u8);
    /// let _: Producer<u8, WaitBusy, true> = producer.set_wait_strategy(WaitBusy);
    /// ```
    #[inline]
    pub fn set_wait_strategy<W2>(self, wait_strategy: W2) -> Producer<E, W2, LEAD> {
        self.inner.set_wait_strategy(wait_strategy).into_producer()
    }

    /// Converts this `Producer` into a [`MultiProducer`].
    ///
    /// # Examples
    /// ```
    /// let (producer, _) = ansa::spsc(64, || 0);
    ///
    /// assert!(matches!(producer.into_multi(), ansa::MultiProducer { .. }))
    /// ```
    #[inline]
    pub fn into_multi(self) -> MultiProducer<E, W, LEAD> {
        let producer_seq = self.sequence();
        MultiProducer {
            inner: self.inner,
            claim: Arc::new(Cursor::new(producer_seq)),
        }
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    #[inline]
    pub fn wait(&mut self, size: usize) -> EventsMut<'_, E> {
        EventsMut(self.inner.wait(size))
    }

    /// Wait until a number of events within the given range are available.
    ///
    /// If a lower bound is provided, it must be less than the buffer size.
    ///
    /// # Examples
    ///
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// producer.wait_range(1..=10); // waits for a batch of at most 10 events
    /// producer.wait_range(10..); // waits for a batch of at least 10 events
    /// producer.wait_range(1..); // waits for a non-zero number of events
    /// ```
    #[inline]
    pub fn wait_range<R>(&mut self, range: R) -> EventsMut<'_, E>
    where
        R: RangeBounds<usize>,
    {
        EventsMut(self.inner.wait_range(range))
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ansa::wait::{Timeout, WaitBusy};
    ///
    /// let (producer, mut consumer) = ansa::spsc(64, || 0u32);
    /// let timeout = Timeout::new(Duration::from_micros(1), WaitBusy);
    /// let mut producer = producer.set_wait_strategy(timeout);
    ///
    /// // move the producer up
    /// producer.try_wait(60).unwrap().consume();
    ///
    /// // try to wait for 5 more events, but only 4 more are available,
    /// // so timeout will occur
    /// let res = producer.try_wait(5);
    /// assert_eq!(res.err(), Some(ansa::wait::TimedOut));
    ///
    /// // make some events available
    /// consumer.wait(10).consume();
    ///
    /// let res = producer.try_wait(5);
    /// assert!(res.is_ok());
    /// ```
    #[inline]
    pub fn try_wait(&mut self, size: usize) -> Result<EventsMut<'_, E>, W::Error> {
        self.inner.try_wait(size).map(EventsMut)
    }

    /// Wait until a number of events within the given range are available.
    ///
    /// If a lower bound is provided, it must be less than the buffer size.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ansa::wait::{Timeout, WaitBusy};
    ///
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    /// let timeout = Timeout::new(Duration::from_millis(1), WaitBusy);
    /// let mut producer = producer.set_wait_strategy(timeout);
    ///
    /// producer.try_wait_range(1..=10)?; // waits for a batch of at most 10 events
    /// producer.try_wait_range(10..)?; // waits for a batch of at least 10 events
    /// producer.try_wait_range(1..)?; // waits for a non-zero number of events
    /// # Ok::<(), ansa::wait::TimedOut>(())
    /// ```
    #[inline]
    pub fn try_wait_range<R>(&mut self, range: R) -> Result<EventsMut<'_, E>, W::Error>
    where
        R: RangeBounds<usize>,
    {
        self.inner.try_wait_range(range).map(EventsMut)
    }
}

/// A handle with immutable access to events on the ring buffer.
///
/// Can access events concurrently to other handles with immutable access.
#[derive(Debug)]
#[repr(transparent)]
pub struct Consumer<E, W> {
    inner: HandleInner<E, W, false>,
}

impl<E, W> Consumer<E, W> {
    /// Returns the current sequence value for this consumer's cursor.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    /// // sequences start at -1, but accesses always occur at the next sequence,
    /// // so the first accessed sequence will be 0
    /// assert_eq!(producer.sequence(), -1);
    ///
    /// // move the producer up by 10, otherwise the consumer will block the
    /// // thread while waiting for available events
    /// producer.wait(10).consume();
    /// assert_eq!(producer.sequence(), 9);
    ///
    /// // now we can move the consumer
    /// consumer.wait(5).consume();
    /// assert_eq!(consumer.sequence(), 4);
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        self.inner.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    ///
    /// # Examples
    /// ```
    /// let (_, consumer) = ansa::spsc(64, || 0);
    /// assert_eq!(consumer.buffer_size(), 64);
    /// ```
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.inner.buffer_size()
    }

    /// Returns a new handle with the given `wait_strategy`, consuming this handle.
    ///
    /// All other properties of the original handle remain unchanged in the new handle.
    ///
    /// Does not alter the wait strategy for any other handle.
    ///
    /// # Examples
    /// ```
    /// use ansa::Consumer;
    /// use ansa::wait::WaitBusy;
    ///
    /// let (_, consumer) = ansa::spsc(64, || 0u8);
    /// let _: Consumer<u8, WaitBusy> = consumer.set_wait_strategy(WaitBusy);
    /// ```
    #[inline]
    pub fn set_wait_strategy<W2>(self, wait_strategy: W2) -> Consumer<E, W2> {
        self.inner.set_wait_strategy(wait_strategy).into_consumer()
    }
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    #[inline]
    pub fn wait(&mut self, size: usize) -> Events<'_, E> {
        Events(self.inner.wait(size))
    }

    /// Wait until a number of events within the given range are available.
    ///
    /// If a lower bound is provided, it must be less than the buffer size.
    ///
    /// Any range which starts from 0 or is unbounded enables non-blocking waits for
    /// [`well-behaved`] [`WaitStrategy`] implementations. For example, `..` will never wait. // todo: test
    ///
    /// # Examples
    ///
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// let events = consumer.wait_range(..); // wait for any number of events
    /// assert_eq!(events.size(), 0);
    ///
    /// // Whereas this would block the thread waiting for the producer to move.
    /// // consumer.wait_range(1..);
    ///
    /// // move the lead producer
    /// producer.wait(20).consume();
    ///
    /// let events = consumer.wait_range(..=10); // wait for a batch of at most 10 events
    /// assert_eq!(events.size(), 10);
    ///
    /// let events = consumer.wait_range(10..); // wait for a batch of at least 10 events
    /// assert_eq!(events.size(), 20);
    ///
    /// let events = consumer.wait_range(..);
    /// assert_eq!(events.size(), 20);
    /// ```
    #[inline]
    pub fn wait_range<R>(&mut self, range: R) -> Events<'_, E>
    where
        R: RangeBounds<usize>,
    {
        Events(self.inner.wait_range(range))
    }
}

impl<E, W> Consumer<E, W>
where
    W: TryWaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ansa::wait::{Timeout, WaitBusy};
    ///
    /// let (mut producer, consumer) = ansa::spsc(64, || 0u32);
    /// let timeout = Timeout::new(Duration::from_micros(1), WaitBusy);
    /// let mut consumer = consumer.set_wait_strategy(timeout);
    ///
    /// // try to wait for 5 events, but none are available, so timeout will occur
    /// let res = consumer.try_wait(5);
    /// assert_eq!(res.err(), Some(ansa::wait::TimedOut));
    ///
    /// // make some events available
    /// producer.wait(10).consume();
    ///
    /// let res = consumer.try_wait(5);
    /// assert!(res.is_ok());
    /// ```
    #[inline]
    pub fn try_wait(&mut self, size: usize) -> Result<Events<'_, E>, W::Error> {
        self.inner.try_wait(size).map(Events)
    }

    /// Wait until a number of events within the given range are available.
    ///
    /// If a lower bound is provided, it must be less than the buffer size.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ansa::wait::{Timeout, WaitBusy};
    ///
    /// let (mut producer, consumer) = ansa::spsc(64, || 0);
    /// // move the lead producer
    /// producer.wait(20).consume();
    ///
    /// let timeout = Timeout::new(Duration::from_millis(1), WaitBusy);
    /// let mut consumer = consumer.set_wait_strategy(timeout);
    ///
    /// consumer.try_wait_range(1..=10)?; // waits for a batch of at most 10 events
    /// consumer.try_wait_range(10..)?; // waits for a batch of at least 10 events
    /// consumer.try_wait_range(1..)?; // waits for a non-zero number of events
    /// # Ok::<(), ansa::wait::TimedOut>(())
    /// ```
    #[inline]
    pub fn try_wait_range<R>(&mut self, range: R) -> Result<Events<'_, E>, W::Error>
    where
        R: RangeBounds<usize>,
    {
        self.inner.try_wait_range(range).map(Events)
    }
}

#[derive(Debug)]
pub(crate) struct HandleInner<E, W, const LEAD: bool> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
    pub(crate) available: i64,
}

impl<E, W> HandleInner<E, W, false> {
    #[inline]
    pub(crate) const fn into_consumer(self) -> Consumer<E, W> {
        Consumer { inner: self }
    }
}

impl<E, W, const LEAD: bool> HandleInner<E, W, LEAD> {
    pub(crate) fn new(
        cursor: Arc<Cursor>,
        barrier: Barrier,
        buffer: Arc<RingBuffer<E>>,
        wait_strategy: W,
    ) -> Self {
        let available = if LEAD {
            // buffer size can always be cast to i64, as allocations cannot be larger than i64::MAX
            CURSOR_START - (buffer.size() as i64)
        } else {
            CURSOR_START
        };
        HandleInner {
            cursor,
            barrier,
            buffer,
            wait_strategy,
            available,
        }
    }

    #[inline]
    pub(crate) const fn into_producer(self) -> Producer<E, W, LEAD> {
        Producer { inner: self }
    }

    #[inline]
    fn buffer_size(&self) -> usize {
        self.buffer.size()
    }

    #[inline]
    fn set_wait_strategy<W2>(self, wait_strategy: W2) -> HandleInner<E, W2, LEAD> {
        HandleInner {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy,
            available: self.available,
        }
    }

    #[inline]
    fn wait_bounds(&self, size: i64) -> (i64, i64) {
        let from_sequence = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = from_sequence + size;
        let till_sequence = if LEAD {
            batch_end - to_i64_saturated(self.buffer.size())
        } else {
            batch_end
        };
        (from_sequence, till_sequence)
    }

    #[inline]
    const fn as_batch(&mut self, from_seq: i64, batch_size: usize) -> Batch<'_, E> {
        Batch {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: from_seq,
            size: batch_size,
        }
    }

    #[inline]
    fn range_batch_size(&self, from_seq: i64, end_bound: Bound<&usize>) -> usize {
        let from = if LEAD {
            from_seq - to_i64_saturated(self.buffer.size())
        } else {
            from_seq
        };
        let available_batch = (self.available - from).unsigned_abs() as usize;
        match end_bound {
            Bound::Included(b) => available_batch.min(*b),
            Bound::Excluded(b) => available_batch.min(b.saturating_sub(1)),
            Bound::Unbounded => available_batch,
        }
    }
}

impl<E, W, const LEAD: bool> HandleInner<E, W, LEAD>
where
    W: WaitStrategy,
{
    #[inline]
    fn wait(&mut self, size: usize) -> Batch<'_, E> {
        debug_assert!(self.buffer.size() >= size);
        let (from_seq, till_seq) = self.wait_bounds(to_i64_saturated(size));
        if self.available < till_seq {
            self.available = self.wait_strategy.wait(till_seq, &self.barrier);
        }
        debug_assert!(self.available >= till_seq);
        self.as_batch(from_seq, size)
    }

    // todo: check that range: 0.. always returns immediately if no sequence available
    #[inline]
    fn wait_range<R>(&mut self, range: R) -> Batch<'_, E>
    where
        R: RangeBounds<usize>,
    {
        let batch_min = match range.start_bound() {
            Bound::Included(b) => *b,
            Bound::Excluded(b) => b.saturating_add(1),
            Bound::Unbounded => 0,
        };
        debug_assert!(self.buffer.size() >= batch_min);
        let (from_seq, till_seq) = self.wait_bounds(to_i64_saturated(batch_min));
        if self.available < till_seq.max(1) {
            self.available = self.wait_strategy.wait(till_seq, &self.barrier);
        }
        debug_assert!(self.available >= till_seq);
        let batch_max = self.range_batch_size(from_seq, range.end_bound());
        self.as_batch(from_seq, batch_max)
    }
}

impl<E, W, const LEAD: bool> HandleInner<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    #[inline]
    fn try_wait(&mut self, size: usize) -> Result<Batch<'_, E>, W::Error> {
        debug_assert!(self.buffer.size() >= size);
        let (from_seq, till_seq) = self.wait_bounds(to_i64_saturated(size));
        if self.available < till_seq {
            self.available = self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        }
        debug_assert!(self.available >= till_seq);
        Ok(self.as_batch(from_seq, size))
    }

    #[inline]
    fn try_wait_range<R>(&mut self, range: R) -> Result<Batch<'_, E>, W::Error>
    where
        R: RangeBounds<usize>,
    {
        let batch_min = match range.start_bound() {
            Bound::Included(b) => *b,
            Bound::Excluded(b) => b.saturating_add(1),
            Bound::Unbounded => 0,
        };
        debug_assert!(self.buffer.size() >= batch_min);
        let (from_seq, till_seq) = self.wait_bounds(to_i64_saturated(batch_min));
        if self.available < till_seq.max(1) {
            self.available = self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        }
        debug_assert!(self.available >= till_seq);
        let batch_max = self.range_batch_size(from_seq, range.end_bound());
        Ok(self.as_batch(from_seq, batch_max))
    }
}

struct Batch<'a, E> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current: i64,
    size: usize,
}

impl<E> Batch<'_, E> {
    #[inline]
    fn apply<F>(self, f: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        // SAFETY: Acquire-Release barrier ensures following handles cannot access this sequence
        // before this handle has finished interacting with it. Construction of the disruptor
        // guarantees producers don't overlap with other handles, thus no mutable aliasing.
        unsafe { self.buffer.apply(self.current + 1, self.size, f) };
        let seq_end = self.current + to_i64_saturated(self.size);
        self.cursor.sequence.store(seq_end, Ordering::Release);
    }

    #[inline]
    fn try_apply<F, Err>(self, f: F) -> Result<(), Err>
    where
        F: FnMut(*mut E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        // SAFETY: see Batch::apply
        unsafe { self.buffer.try_apply(self.current + 1, self.size, f)? };
        let seq_end = self.current + to_i64_saturated(self.size);
        self.cursor.sequence.store(seq_end, Ordering::Release);
        Ok(())
    }

    #[inline]
    fn try_commit<F, Err>(self, mut f: F) -> Result<(), Err>
    where
        F: FnMut(*mut E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        // SAFETY: see Batch::apply
        unsafe {
            self.buffer.try_apply(self.current + 1, self.size, |ptr, seq, end| {
                f(ptr, seq, end)
                    .inspect_err(|_| self.cursor.sequence.store(seq - 1, Ordering::Release))
            })?;
        }
        let seq_end = self.current + to_i64_saturated(self.size);
        self.cursor.sequence.store(seq_end, Ordering::Release);
        Ok(())
    }

    #[inline]
    fn consume(self) {
        let seq_end = self.current + to_i64_saturated(self.size);
        self.cursor.sequence.store(seq_end, Ordering::Release);
    }
}

// todo: elucidate docs with 'in another process' statements, eg. for describing mut alias possibility

/// A batch of events which may be mutably accessed.
#[repr(transparent)]
pub struct EventsMut<'a, E>(Batch<'a, E>);

impl<E> EventsMut<'_, E> {
    /// Returns the size of this batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// let events = producer.wait(10);
    /// assert_eq!(events.size(), 10);
    /// ```
    #[inline]
    pub const fn size(&self) -> usize {
        self.0.size
    }

    /// Process a batch of mutable events on the buffer using the closure `f`.
    ///
    /// The parameters of `f` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// producer.wait(10).for_each(|event, seq, _| *event = seq);
    /// ```
    #[inline]
    pub fn for_each<F>(self, mut f: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        self.0.apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &mut E = unsafe { &mut *ptr };
            f(event, seq, end)
        })
    }

    /// Try to process a batch of mutable events on the buffer using the closure `f`.
    ///
    /// If an error occurs, leaves the cursor sequence unchanged and returns the error.
    ///
    /// **Important**: does _not_ undo any mutations to events if an error occurs.
    ///
    /// The parameters of `f` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// producer.wait(10).try_for_each(|_, seq, _| {
    ///     match seq {
    ///         100 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// })?;
    /// assert_eq!(producer.sequence(), 9);
    ///
    /// let result = producer.wait(10).try_for_each(|_, seq, _| {
    ///     match seq {
    ///         15 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// });
    /// assert_eq!(result, Err(15));
    /// // If an error is returned, the cursor will not be moved.
    /// assert_eq!(producer.sequence(), 9);
    /// # Ok::<(), i64>(())
    /// ```
    #[inline]
    pub fn try_for_each<F, Err>(self, mut f: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &mut E = unsafe { &mut *ptr };
            f(event, seq, end)
        })
    }

    /// Try to process a batch of mutable events on the buffer using the closure `f`.
    ///
    /// If an error occurs, returns the error and updates the cursor sequence to the position of
    /// the last successfully processed event. In effect, commits the successful portion of the batch.
    ///
    /// **Important**: does _not_ undo any mutations to events if an error occurs.
    ///
    /// The parameters of `f` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// producer.wait(10).try_commit_each(|_, seq, _| {
    ///     match seq {
    ///         100 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// })?;
    ///
    /// assert_eq!(producer.sequence(), 9);
    /// # Ok::<(), i64>(())
    /// ```
    /// On failure, the cursor will be moved to the last successful sequence.
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// let result = producer.wait(10).try_commit_each(|_, seq, _| {
    ///     match seq {
    ///         5 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// });
    ///
    /// assert_eq!(result, Err(5));
    /// assert_eq!(producer.sequence(), 4);
    /// ```
    #[inline]
    pub fn try_commit_each<F, Err>(self, mut f: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_commit(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &mut E = unsafe { &mut *ptr };
            f(event, seq, end)
        })
    }

    /// Moves the handle forward without applying a function, which consumes the batch without
    /// accessing its events.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0);
    ///
    /// assert_eq!(producer.sequence(), -1);
    /// producer.wait(10).consume();
    /// assert_eq!(producer.sequence(), 9);
    /// ```
    #[inline]
    pub fn consume(self) {
        self.0.consume()
    }
}

/// A batch of events which may be immutably accessed.
#[repr(transparent)]
pub struct Events<'a, E>(Batch<'a, E>);

impl<E> Events<'_, E> {
    /// Returns the size of this batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// let events = producer.wait(10);
    /// assert_eq!(events.size(), 10);
    /// // move the producer up
    /// events.consume();
    ///
    /// // attempt to wait for upto 20 events
    /// let less_events = consumer.wait_range(..20);
    /// // we only get the 10 available back
    /// assert_eq!(less_events.size(), 10);
    /// ```
    #[inline]
    pub const fn size(&self) -> usize {
        self.0.size
    }

    /// Process a batch of immutable events on the buffer using the closure `f`.
    ///
    /// The parameters of `f` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// // move the producer so that events are available to the following consumer
    /// producer.wait(20).consume();
    ///
    /// consumer.wait(10).for_each(|event, seq, _| println!("{seq}: {event}"));
    /// ```
    #[inline]
    pub fn for_each<F>(self, mut f: F)
    where
        F: FnMut(&E, i64, bool),
    {
        self.0.apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &E = unsafe { &*ptr };
            f(event, seq, end)
        })
    }

    /// Try to process a batch of immutable events on the buffer using the closure `f`.
    ///
    /// If an error occurs, leaves the cursor sequence unchanged and returns the error.
    ///
    /// The parameters of `f` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// // move the producer so that events are available to the following consumer
    /// producer.wait(20).consume();
    ///
    /// consumer.wait(10).try_for_each(|_, seq, _| {
    ///     match seq {
    ///         100 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// })?;
    ///
    /// assert_eq!(consumer.sequence(), 9);
    /// # Ok::<(), i64>(())
    /// ```
    /// On failure, the cursor will not be moved.
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// // move the producer so that events are available to the following consumer
    /// producer.wait(20).consume();
    ///
    /// let result = consumer.wait(10).try_for_each(|_, seq, _| {
    ///     match seq {
    ///         5 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// });
    ///
    /// assert_eq!(result, Err(5));
    /// // sequence values start at -1, and the first event is at sequence 0
    /// assert_eq!(consumer.sequence(), -1);
    /// ```
    #[inline]
    pub fn try_for_each<F, Err>(self, mut f: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &E = unsafe { &*ptr };
            f(event, seq, end)
        })
    }

    /// Try to process a batch of immutable events on the buffer using the closure `f`.
    ///
    /// If an error occurs, returns the error and updates the cursor sequence to the position of
    /// the last successfully processed event. In effect, commits the successful portion of the batch.
    ///
    /// The parameters of `f` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// // move the producer so that events are available to the following consumer
    /// producer.wait(20).consume();
    ///
    /// consumer.wait(10).try_commit_each(|_, seq, _| {
    ///     match seq {
    ///         100 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// })?;
    ///
    /// assert_eq!(consumer.sequence(), 9);
    /// # Ok::<(), i64>(())
    /// ```
    /// On failure, the cursor will be moved to the last successful sequence.
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// // move the producer so that events are available to the following consumer
    /// producer.wait(20).consume();
    ///
    /// let result = consumer.wait(10).try_commit_each(|_, seq, _| {
    ///     match seq {
    ///         5 => Err(seq),
    ///         _ => Ok(())
    ///     }
    /// });
    ///
    /// assert_eq!(result, Err(5));
    /// assert_eq!(consumer.sequence(), 4);
    /// ```
    #[inline]
    pub fn try_commit_each<F, Err>(self, mut f: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_commit(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &E = unsafe { &*ptr };
            f(event, seq, end)
        })
    }

    /// Moves the handle forward without applying a function, which consumes the batch without
    /// accessing its events.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, mut consumer) = ansa::spsc(64, || 0);
    ///
    /// producer.wait(10).consume();
    ///
    /// assert_eq!(consumer.sequence(), -1);
    /// // attempt to consume upto 20 events
    /// consumer.wait_range(..20).consume();
    /// // the 10 available events are consumed
    /// assert_eq!(consumer.sequence(), 9);
    /// ```
    #[inline]
    pub fn consume(self) {
        self.0.consume()
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Cursor {
    #[cfg(not(feature = "cache-padded"))]
    sequence: AtomicI64,
    #[cfg(feature = "cache-padded")]
    sequence: crossbeam_utils::CachePadded<AtomicI64>,
}

const CURSOR_START: i64 = -1;

impl Cursor {
    pub(crate) const fn new(seq: i64) -> Self {
        Cursor {
            #[cfg(not(feature = "cache-padded"))]
            sequence: AtomicI64::new(seq),
            #[cfg(feature = "cache-padded")]
            sequence: crossbeam_utils::CachePadded::new(AtomicI64::new(seq)),
        }
    }

    /// Create a cursor at the start of the sequence. All accesses begin on the _next_ position in
    /// the sequence, thus cursors start at `-1`, so that accesses start at `0`.
    pub(crate) const fn start() -> Self {
        Cursor::new(CURSOR_START)
    }
}

/// A collection of cursors that determine which sequence is available to a handle.
///
/// Every handle has a barrier, and no handle may overtake its barrier.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Barrier(BarrierInner);

#[derive(Clone, Debug)]
enum BarrierInner {
    One(Arc<Cursor>),
    Many(Box<[Arc<Cursor>]>),
}

impl Barrier {
    pub(crate) const fn one(cursor: Arc<Cursor>) -> Self {
        Barrier(BarrierInner::One(cursor))
    }

    pub(crate) const fn many(cursors: Box<[Arc<Cursor>]>) -> Self {
        Barrier(BarrierInner::Many(cursors))
    }

    /// The position of the barrier.
    ///
    /// # Examples
    /// ```
    /// use ansa::{Barrier, wait::WaitStrategy};
    ///
    /// struct MyBusyWait;
    ///
    /// // SAFETY: wait returns once barrier seq >= desired_seq, with barrier seq itself
    /// unsafe impl WaitStrategy for MyBusyWait {
    ///     fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
    ///         while barrier.sequence() < desired_seq {} // busy-spin
    ///         barrier.sequence()
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        match &self.0 {
            BarrierInner::One(cursor) => cursor.sequence.load(Ordering::Relaxed),
            BarrierInner::Many(cursors) => cursors.iter().fold(i64::MAX, |seq, cursor| {
                seq.min(cursor.sequence.load(Ordering::Relaxed))
            }),
        }
    }
}

impl Drop for Barrier {
    // We need to break the Arc cycle of barriers. Just get rid of all the Arcs to guarantee this.
    fn drop(&mut self) {
        self.0 = BarrierInner::Many(Box::new([]));
    }
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
#[inline]
const fn to_i64_saturated(u: usize) -> i64 {
    if const { size_of::<usize>() >= 8 } {
        // the 64th bit must be zero to avoid negative wrapping when casting to i64
        (u & i64::MAX as usize) as i64
    } else {
        // all usize values fit in i64
        u as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait::WaitBusy;

    // Check that sizes don't accidentally change. If size change is found and intended, just
    // change the values in this test.
    #[test]
    fn sizes() {
        assert_eq!(size_of::<Consumer<u8, WaitBusy>>(), 40);
        assert_eq!(size_of::<Producer<u8, WaitBusy, true>>(), 40);
        assert_eq!(size_of::<MultiProducer<u8, WaitBusy, true>>(), 48);
    }

    #[test]
    fn test_wait_range() {
        let buffer = Arc::new(RingBuffer::from_factory(16, || 0));
        let lead_cursor = Arc::new(Cursor::new(8));
        let follows_cursor = Arc::new(Cursor::new(4));

        let mut lead_handle = HandleInner::<_, _, true>::new(
            Arc::clone(&lead_cursor),
            Barrier::one(Arc::clone(&follows_cursor)),
            Arc::clone(&buffer),
            WaitBusy,
        );

        let mut follows_handle = HandleInner::<_, _, false>::new(
            Arc::clone(&follows_cursor),
            Barrier::one(Arc::clone(&lead_cursor)),
            Arc::clone(&buffer),
            WaitBusy,
        );

        let lead_batch = lead_handle.wait_range(1..);
        assert_eq!(lead_batch.current, 8);
        assert_eq!(lead_batch.size, 12);

        let follows_batch = follows_handle.wait_range(1..);
        assert_eq!(follows_batch.current, 4);
        assert_eq!(follows_batch.size, 4);
    }

    #[test]
    fn test_size_zero_apply() {
        let buffer = Arc::new(RingBuffer::from_factory(16, || 0));
        let mut lead_handle = HandleInner::<_, _, true>::new(
            Arc::new(Cursor::new(8)),
            Barrier::one(Arc::new(Cursor::new(4))),
            Arc::clone(&buffer),
            WaitBusy,
        );

        let sequence_before_apply = lead_handle.cursor.sequence.load(Ordering::Relaxed);

        let batch = lead_handle.wait(0);
        assert_eq!(batch.size, 0);

        batch.apply(|_, _, _| assert!(false));
        // sequence should not have moved and the apply function should not have been called
        let sequence_after_apply = lead_handle.cursor.sequence.load(Ordering::Relaxed);
        assert_eq!(sequence_before_apply, sequence_after_apply);
    }
}
