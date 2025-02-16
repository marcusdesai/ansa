use crate::ringbuffer::RingBuffer;
use crate::wait::{TryWaitStrategy, WaitStrategy};
use std::sync::atomic::{fence, AtomicI64, Ordering};
use std::sync::Arc;

/// A handle with mutable access to events on the ring buffer.
///
/// Cannot access events concurrently with other handles.
///
/// `MultiProducer`s can be cloned to enable distributed writes. Clones coordinate by claiming
/// non-overlapping ranges of sequence values, which can be writen to in parallel.
///
/// Clones of this `MultiProducer` share this producer's cursor.
///
/// # Limitations
///
/// `MultiProducer` clones coordinate by claiming exact ranges of sequences that they alone will
/// access. Since this claim occurs before waiting for sequence availability, the `MultiProducer`
/// cannot flexibly change how much it can access depending on the actual sequence availability, as
/// it must stick to the claimed range. The result is that the methods: `wait_min`, `wait_any` and
/// `wait_max` (and `try` variants) cannot be implemented for `MultiProducer`s.
///
/// # Examples
/// ```
/// use ansa::{DisruptorBuilder, Follows, Handle};
///
/// let mut handles = DisruptorBuilder::new(64, || 0)
///     .add_handle(0, Handle::Producer, Follows::LeadProducer)
///     .build()?;
///
/// // lead and trailing are separate handles with separate cursors and clones
/// let lead = handles.take_lead().unwrap().into_multi();
/// let trailing = handles.take_producer(0).unwrap().into_multi();
///
/// let lead_clone = lead.clone();
///
/// assert_eq!(lead.count(), 2);
/// assert_eq!(trailing.count(), 1);
/// # Ok::<(), ansa::BuildError>(())
/// ```
#[derive(Debug)]
pub struct MultiProducer<E, W, const LEAD: bool> {
    cursor: Arc<Cursor>,
    barrier: Barrier,
    buffer: Arc<RingBuffer<E>>,
    claim: Arc<Cursor>, // shared between all clones of this multi producer
    wait_strategy: W,
}

impl<E, W, const LEAD: bool> Clone for MultiProducer<E, W, LEAD>
where
    W: Clone,
{
    fn clone(&self) -> Self {
        MultiProducer {
            cursor: Arc::clone(&self.cursor),
            barrier: self.barrier.clone(),
            buffer: Arc::clone(&self.buffer),
            claim: Arc::clone(&self.claim),
            wait_strategy: self.wait_strategy.clone(),
        }
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD> {
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
    /// multi.wait(10).write(|_, _, _| ());
    /// assert_eq!(multi.sequence(), 9);
    ///
    /// // clone and move only the clone
    /// let mut clone = multi.clone();
    /// clone.wait(10).write(|_, _, _| ());
    ///
    /// // Both have moved because all clones share the same cursor
    /// assert_eq!(multi.sequence(), 19);
    /// assert_eq!(clone.sequence(), 19);
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
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
        self.buffer.size()
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
        Arc::into_inner(self.claim).map(|_| Producer {
            handle: HandleInner {
                cursor: self.cursor,
                barrier: self.barrier,
                buffer: self.buffer,
                wait_strategy: self.wait_strategy,
            },
        })
    }

    #[inline]
    fn wait_range(&self, size: i64) -> (i64, i64) {
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
            claim_end - self.buffer.size() as i64
        } else {
            claim_end
        };
        (current_claim, desired_seq)
    }

    #[inline]
    fn as_available(&mut self, from_seq: i64, batch_size: i64) -> AvailableWrite<'_, E> {
        AvailableWrite(Available {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: from_seq,
            batch_size,
            set_cursor: |cursor, current, end, ordering| {
                // Busy wait for the cursor to catch up to the start of claimed sequence.
                while cursor.sequence.load(Ordering::Acquire) != current {}
                cursor.sequence.store(end, ordering)
            },
        })
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    pub fn wait(&mut self, size: u32) -> AvailableWrite<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(size as i64);
        self.wait_strategy.wait(till_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= till_seq);
        self.as_available(from_seq, size as i64)
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(size as i64);
        self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= till_seq);
        Ok(self.as_available(from_seq, size as i64))
    }
}

/// A handle with mutable access to events on the ring buffer.
///
/// Cannot access events concurrently with other handles.
#[derive(Debug)]
#[repr(transparent)]
pub struct Producer<E, W, const LEAD: bool> {
    handle: HandleInner<E, W, LEAD>,
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD> {
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
    /// producer.wait(10).write(|_, _, _| ());
    /// assert_eq!(producer.sequence(), 9);
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        self.handle.cursor.sequence.load(Ordering::Relaxed)
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
        self.handle.buffer.size()
    }

    /// Converts this `Producer` into a [`MultiProducer`].
    #[inline]
    pub fn into_multi(self) -> MultiProducer<E, W, LEAD> {
        let producer_seq = self.handle.cursor.sequence.load(Ordering::Relaxed);
        MultiProducer {
            cursor: self.handle.cursor,
            barrier: self.handle.barrier,
            buffer: self.handle.buffer,
            claim: Arc::new(Cursor::new(producer_seq)),
            wait_strategy: self.handle.wait_strategy,
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
    pub fn wait(&mut self, size: u32) -> AvailableWrite<'_, E> {
        AvailableWrite(self.handle.wait(size))
    }

    /// Wait until at least `size` number of events are available.
    pub fn wait_min(&mut self, size: u32) -> AvailableWrite<'_, E> {
        AvailableWrite(self.handle.wait_min(size))
    }

    /// Wait until any number of events are available.
    pub fn wait_any(&mut self) -> AvailableWrite<'_, E> {
        AvailableWrite(self.handle.wait_any())
    }

    /// Wait until at most `size` number of events are available.
    pub fn wait_max(&mut self, size: u32) -> AvailableWrite<'_, E> {
        AvailableWrite(self.handle.wait_max(size))
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
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        self.handle.try_wait(size).map(AvailableWrite)
    }

    /// Wait until at least `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_min(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        self.handle.try_wait_min(size).map(AvailableWrite)
    }

    /// Wait until any number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_any(&mut self) -> Result<AvailableWrite<'_, E>, W::Error> {
        self.handle.try_wait_any().map(AvailableWrite)
    }

    /// Wait until at most `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_upto(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        self.handle.try_wait_max(size).map(AvailableWrite)
    }
}

/// A handle with immutable access to events on the ring buffer.
///
/// Can access events concurrently to other handles with immutable access.
#[derive(Debug)]
#[repr(transparent)]
pub struct Consumer<E, W> {
    handle: HandleInner<E, W, false>,
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
    /// // thread while waiting
    /// producer.wait(10).write(|_, _, _| ());
    /// assert_eq!(producer.sequence(), 9);
    ///
    /// // now we can move the consumer
    /// consumer.wait(5).read(|_, _, _| ());
    /// assert_eq!(consumer.sequence(), 4);
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        self.handle.cursor.sequence.load(Ordering::Relaxed)
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
        self.handle.buffer.size()
    }
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategy,
{
    /// Wait until exactly `size` number of events are available.
    ///
    /// `size` values larger than the buffer will cause permanent stalls.
    pub fn wait(&mut self, size: u32) -> AvailableRead<'_, E> {
        AvailableRead(self.handle.wait(size))
    }

    /// Wait until at least `size` number of events are available.
    pub fn wait_min(&mut self, size: u32) -> AvailableRead<'_, E> {
        AvailableRead(self.handle.wait_min(size))
    }

    /// Wait until any number of events are available.
    pub fn wait_any(&mut self) -> AvailableRead<'_, E> {
        AvailableRead(self.handle.wait_any())
    }

    /// Wait until at most `size` number of events are available.
    pub fn wait_max(&mut self, size: u32) -> AvailableRead<'_, E> {
        AvailableRead(self.handle.wait_max(size))
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
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableRead<'_, E>, W::Error> {
        self.handle.try_wait(size).map(AvailableRead)
    }

    /// Wait until at least `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_min(&mut self, size: u32) -> Result<AvailableRead<'_, E>, W::Error> {
        self.handle.try_wait_min(size).map(AvailableRead)
    }

    /// Wait until any number of events are available.
    ///
    /// Otherwise, returns the wait strategy error.
    pub fn try_wait_any(&mut self) -> Result<AvailableRead<'_, E>, W::Error> {
        self.handle.try_wait_any().map(AvailableRead)
    }

    /// Wait until at most `size` number of events are available.
    ///
    /// Otherwise, returns the wait strategy error.
    pub fn try_wait_max(&mut self, size: u32) -> Result<AvailableRead<'_, E>, W::Error> {
        self.handle.try_wait_max(size).map(AvailableRead)
    }
}

#[derive(Debug)]
pub(crate) struct HandleInner<E, W, const LEAD: bool> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W> HandleInner<E, W, false> {
    pub(crate) fn consumer(self) -> Consumer<E, W> {
        Consumer { handle: self }
    }
}

impl<E, W, const LEAD: bool> HandleInner<E, W, LEAD> {
    pub(crate) fn producer(self) -> Producer<E, W, LEAD> {
        Producer { handle: self }
    }

    #[inline]
    fn wait_range(&self, size: i64) -> (i64, i64) {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = sequence + size;
        let desired_seq = if LEAD {
            batch_end - self.buffer.size() as i64
        } else {
            batch_end
        };
        (sequence, desired_seq)
    }

    #[inline]
    fn as_available(&mut self, from_seq: i64, batch_size: i64) -> Available<'_, E> {
        Available {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: from_seq,
            batch_size,
            set_cursor: |cursor, _, end, ordering| cursor.sequence.store(end, ordering),
        }
    }
}

impl<E, W, const LEAD: bool> HandleInner<E, W, LEAD>
where
    W: WaitStrategy,
{
    #[inline]
    fn wait(&mut self, size: u32) -> Available<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(size as i64);
        self.wait_strategy.wait(till_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= till_seq);
        self.as_available(from_seq, size as i64)
    }

    #[inline]
    fn wait_min(&mut self, size: u32) -> Available<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(size as i64);
        let barrier_seq = self.wait_strategy.wait(till_seq, &self.barrier);
        debug_assert!(barrier_seq >= till_seq);
        self.as_available(from_seq, barrier_seq - from_seq)
    }

    #[inline]
    fn wait_any(&mut self) -> Available<'_, E> {
        let (from_seq, till_seq) = self.wait_range(1);
        let barrier_seq = self.wait_strategy.wait(till_seq, &self.barrier);
        debug_assert!(barrier_seq > from_seq);
        self.as_available(from_seq, barrier_seq - from_seq)
    }

    #[inline]
    fn wait_max(&mut self, size: u32) -> Available<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(1);
        let barrier_seq = self.wait_strategy.wait(till_seq, &self.barrier);
        debug_assert!(barrier_seq > from_seq);
        let batch_size = (barrier_seq - from_seq).min(size as i64);
        self.as_available(from_seq, batch_size)
    }
}

impl<E, W, const LEAD: bool> HandleInner<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    #[inline]
    fn try_wait(&mut self, size: u32) -> Result<Available<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(size as i64);
        self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= till_seq);
        Ok(self.as_available(from_seq, size as i64))
    }

    #[inline]
    fn try_wait_min(&mut self, size: u32) -> Result<Available<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(size as i64);
        let barrier_seq = self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        debug_assert!(barrier_seq >= till_seq);
        Ok(self.as_available(from_seq, barrier_seq - from_seq))
    }

    #[inline]
    fn try_wait_any(&mut self) -> Result<Available<'_, E>, W::Error> {
        let (from_seq, till_seq) = self.wait_range(1);
        let barrier_seq = self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        debug_assert!(barrier_seq > from_seq);
        Ok(self.as_available(from_seq, barrier_seq - from_seq))
    }

    #[inline]
    fn try_wait_max(&mut self, size: u32) -> Result<Available<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (from_seq, till_seq) = self.wait_range(1);
        let barrier_seq = self.wait_strategy.try_wait(till_seq, &self.barrier)?;
        debug_assert!(barrier_seq > from_seq);
        let batch_size = (barrier_seq - from_seq).min(size as i64);
        Ok(self.as_available(from_seq, batch_size))
    }
}

struct Available<'a, E> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current: i64,
    batch_size: i64,
    set_cursor: fn(&Arc<Cursor>, i64, i64, Ordering),
}

impl<E> Available<'_, E> {
    fn apply<F>(self, f: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        // SAFETY: Acquire-Release barrier ensures that following handles cannot access this
        // sequence before the current handle has finished interacting with it. The construction of
        // the disruptor guarantees no producer & consumer overlap, thus no mutable aliasing.
        unsafe { self.buffer.apply(self.current + 1, self.batch_size, f) };
        let seq_end = self.current + self.batch_size;
        (self.set_cursor)(self.cursor, self.current, seq_end, Ordering::Release);
    }

    fn try_apply<F, Err>(self, f: F) -> Result<(), Err>
    where
        F: FnMut(*mut E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        // SAFETY: Acquire-Release barrier ensures that following handles cannot access this
        // sequence before the current handle has finished interacting with it. The construction of
        // the disruptor guarantees no producer & consumer overlap, thus no mutable aliasing.
        unsafe { self.buffer.try_apply(self.current + 1, self.batch_size, f)? };
        let seq_end = self.current + self.batch_size;
        (self.set_cursor)(self.cursor, self.current, seq_end, Ordering::Release);
        Ok(())
    }

    fn try_apply_commit<F, Err>(self, mut f: F) -> Result<(), Err>
    where
        F: FnMut(*mut E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        // SAFETY: Acquire-Release barrier ensures that following handles cannot access this
        // sequence before the current handle has finished interacting with it. The construction of
        // the disruptor guarantees no producer & consumer overlap, thus no mutable aliasing.
        unsafe {
            self.buffer.try_apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                f(ptr, seq, end).inspect_err(|_| {
                    (self.set_cursor)(self.cursor, self.current, seq - 1, Ordering::Release);
                })
            })?;
        }
        let seq_end = self.current + self.batch_size;
        (self.set_cursor)(self.cursor, self.current, seq_end, Ordering::Release);
        Ok(())
    }
}

// todo: elucidate docs with 'in another process' statements, eg. for describing mut alias possibility

/// Represents a range of available sequences which may be written to.
#[repr(transparent)]
pub struct AvailableWrite<'a, E>(Available<'a, E>);

impl<E> AvailableWrite<'_, E> {
    /// Write a batch of events to the buffer.
    ///
    /// The parameters of `write` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0u32);
    /// // obtain `AvailableWrite` by waiting
    /// producer.wait(10).write(|event, seq, _| *event = seq as u32);
    /// ```
    pub fn write<F>(self, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        self.0.apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &mut E = unsafe { &mut *ptr };
            write(event, seq, end)
        })
    }

    /// Write an exact batch of events to the buffer if successful.
    ///
    /// Otherwise, leave cursor sequence unchanged and return the error. **Important**: does _not_
    /// undo successful writes.
    ///
    /// The parameters of `write` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0u32);
    /// // obtain `AvailableWrite` by waiting
    /// producer.wait(10).try_write(|event, seq, _| {
    ///     if seq == 100 {
    ///         return Err(seq);
    ///     }
    ///     *event = seq as u32;
    ///     Ok(())
    /// })?;
    ///
    /// assert_eq!(producer.sequence(), 9);
    /// # Ok::<(), i64>(())
    /// ```
    /// Sequence will be rolled back on failure.
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0u32);
    ///
    /// let result = producer.wait(10).try_write(|event, seq, _| {
    ///     if seq == 5 {
    ///         return Err(seq);
    ///     }
    ///     *event = seq as u32;
    ///     Ok(())
    /// });
    /// assert_eq!(result, Err(5));
    /// // sequence values start at -1
    /// assert_eq!(producer.sequence(), -1);
    /// ```
    pub fn try_write<F, Err>(self, mut write: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &mut E = unsafe { &mut *ptr };
            write(event, seq, end)
        })
    }

    /// Write an exact batch of events to the buffer if successful.
    ///
    /// Otherwise, return the error and update cursor sequence to the position of the last
    /// successful write. In effect, commits successful portion of the batch.
    ///
    /// The parameters of `write` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    ///
    /// # Examples
    /// ```
    /// let (mut producer, _) = ansa::spsc(64, || 0u32);
    /// // obtain `AvailableWrite` by waiting
    /// producer.wait(10).write(|event, seq, _| *event = seq as u32);
    /// ```
    pub fn try_write_commit<F, Err>(self, mut write: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_apply_commit(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &mut E = unsafe { &mut *ptr };
            write(event, seq, end)
        })
    }
}

/// Represents a range of available sequences which may be read from.
#[repr(transparent)]
pub struct AvailableRead<'a, E>(Available<'a, E>);

impl<E> AvailableRead<'_, E> {
    /// Read a batch of events from the buffer.
    ///
    /// The parameters of `read` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    pub fn read<F>(self, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        self.0.apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &E = unsafe { &mut *ptr };
            read(event, seq, end)
        })
    }

    /// Read a batch of events from the buffer if successful.
    ///
    /// Otherwise, leave cursor sequence unchanged and return the error. Effectively returns to the
    /// start of the batch.
    ///
    /// The parameters of `read` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    pub fn try_read<F, Err>(self, mut read: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_apply(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &E = unsafe { &mut *ptr };
            read(event, seq, end)
        })
    }

    /// Read a batch of events from the buffer if successful.
    ///
    /// Otherwise, return the error and update cursor sequence to the position of the last
    /// successful read. In effect, commits successful portion of the batch.
    ///
    /// The parameters of `read` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    pub fn try_read_commit<F, Err>(self, mut read: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        self.0.try_apply_commit(|ptr, seq, end| {
            // SAFETY: deref guaranteed safe by ringbuffer initialisation
            let event: &E = unsafe { &mut *ptr };
            read(event, seq, end)
        })
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
        Cursor::new(-1)
    }
}

/// A collection of cursors that determine which sequence is available to a handle.
///
/// Every handle has a barrier, and no handle may overtake its barrier.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Barrier(Barrier_);

#[derive(Clone, Debug)]
enum Barrier_ {
    One(Arc<Cursor>),
    Many(Box<[Arc<Cursor>]>),
}

impl Barrier {
    pub(crate) fn one(cursor: Arc<Cursor>) -> Self {
        Barrier(Barrier_::One(cursor))
    }

    pub(crate) fn many(cursors: Box<[Arc<Cursor>]>) -> Self {
        Barrier(Barrier_::Many(cursors))
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
    ///         while desired_seq > barrier.sequence() {} // busy-spin
    ///         barrier.sequence()
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn sequence(&self) -> i64 {
        match &self.0 {
            Barrier_::One(cursor) => cursor.sequence.load(Ordering::Relaxed),
            Barrier_::Many(cursors) => cursors.iter().fold(i64::MAX, |seq, cursor| {
                seq.min(cursor.sequence.load(Ordering::Relaxed))
            }),
        }
    }
}

impl Drop for Barrier {
    // We need to break the Arc cycle of barriers. Just get rid of all the Arcs to guarantee this.
    fn drop(&mut self) {
        self.0 = Barrier_::Many(Box::new([]));
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
        assert_eq!(size_of::<Consumer<u8, WaitBusy>>(), 32);
        assert_eq!(size_of::<Producer<u8, WaitBusy, true>>(), 32);
        assert_eq!(size_of::<MultiProducer<u8, WaitBusy, true>>(), 40);
    }
}
