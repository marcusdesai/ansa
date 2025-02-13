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
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.claim)
    }

    /// Returns the current sequence value for this producer's cursor.
    ///
    /// **Important:** The cursor for a `MultiProducer` is shared by all of its clones. Any of
    /// these clones could alter this value at any time, if they are writing to the buffer.
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
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
    pub fn into_producer(self) -> Option<Producer<E, W, LEAD>> {
        Arc::into_inner(self.claim).map(|_| Producer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }

    /// Returns a claim for a range of sequences.
    ///
    /// The claimed range is exclusive to a single multi producer clone.
    #[inline]
    fn claim_batch(&self, size: i64) -> (i64, i64) {
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
        (current_claim, claim_end)
    }

    #[inline]
    fn batch_range(&self, size: i64) -> (i64, i64) {
        let (current_claim, claim_end) = self.claim_batch(size);
        let desired_seq = if LEAD {
            claim_end - self.buffer.size() as i64
        } else {
            claim_end
        };
        (current_claim, desired_seq)
    }

    #[inline]
    fn as_available_write(&mut self, sequence: i64, batch_size: i64) -> AvailableWrite<'_, E> {
        AvailableWrite {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: sequence,
            batch_size,
            move_cursor: |cursor, current, end, ordering| {
                // Busy wait for the cursor to catch up to the start of claimed sequence.
                while cursor.sequence.load(Ordering::Acquire) != current {}
                cursor.sequence.store(end, ordering)
            },
        }
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer may take longer to become available.
    pub fn wait(&mut self, size: u32) -> AvailableWrite<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (producer_seq, desired_seq) = self.batch_range(size as i64);
        self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= desired_seq);
        self.as_available_write(producer_seq, size as i64)
    }

    /// Wait until any number of events are available.
    pub fn wait_any(&mut self) -> AvailableWrite<'_, E> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(barrier_seq > producer_seq);
        self.as_available_write(producer_seq, barrier_seq - producer_seq)
    }

    /// Wait until at most `size` number of events are available.
    pub fn wait_upto(&mut self, size: u32) -> AvailableWrite<'_, E> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(barrier_seq > producer_seq);
        let batch_size = (barrier_seq - producer_seq).min(size as i64);
        self.as_available_write(producer_seq, batch_size)
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer may take longer to become available.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (producer_seq, desired_seq) = self.batch_range(size as i64);
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= desired_seq);
        Ok(self.as_available_write(producer_seq, size as i64))
    }

    /// Wait until any number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_any(&mut self) -> Result<AvailableWrite<'_, E>, W::Error> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(barrier_seq > producer_seq);
        Ok(self.as_available_write(producer_seq, barrier_seq - producer_seq))
    }

    /// Wait until at most `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_upto(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(barrier_seq > producer_seq);
        let batch_size = (barrier_seq - producer_seq).min(size as i64);
        Ok(self.as_available_write(producer_seq, batch_size))
    }
}

/// A handle with mutable access to events on the ring buffer.
///
/// Cannot access events concurrently with other handles.
#[derive(Debug)]
pub struct Producer<E, W, const LEAD: bool> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD> {
    /// Returns the current sequence value for this producer's cursor.
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer.size()
    }

    /// Converts this `Producer` into a [`MultiProducer`].
    pub fn into_multi(self) -> MultiProducer<E, W, LEAD> {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        MultiProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            claim: Arc::new(Cursor::new(producer_seq)),
            wait_strategy: self.wait_strategy,
        }
    }

    #[inline]
    fn batch_range(&self, size: i64) -> (i64, i64) {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + size;
        let desired_seq = if LEAD {
            batch_end - self.buffer.size() as i64
        } else {
            batch_end
        };
        (producer_seq, desired_seq)
    }

    #[inline]
    fn as_available_write(&mut self, sequence: i64, batch_size: i64) -> AvailableWrite<'_, E> {
        AvailableWrite {
            cursor: &mut self.cursor,
            buffer: &mut self.buffer,
            current: sequence,
            batch_size,
            move_cursor: |cursor, _, end, ordering| cursor.sequence.store(end, ordering),
        }
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer may take longer to become available.
    pub fn wait(&mut self, size: u32) -> AvailableWrite<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (producer_seq, desired_seq) = self.batch_range(size as i64);
        self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= desired_seq);
        self.as_available_write(producer_seq, size as i64)
    }

    /// Wait until any number of events are available.
    pub fn wait_any(&mut self) -> AvailableWrite<'_, E> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(barrier_seq > producer_seq);
        self.as_available_write(producer_seq, barrier_seq - producer_seq)
    }

    /// Wait until at most `size` number of events are available.
    pub fn wait_upto(&mut self, size: u32) -> AvailableWrite<'_, E> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(barrier_seq > producer_seq);
        let batch_size = (barrier_seq - producer_seq).min(size as i64);
        self.as_available_write(producer_seq, batch_size)
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer may take longer to become available.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (producer_seq, desired_seq) = self.batch_range(size as i64);
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= desired_seq);
        Ok(self.as_available_write(producer_seq, size as i64))
    }

    /// Wait until any number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_any(&mut self) -> Result<AvailableWrite<'_, E>, W::Error> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(barrier_seq > producer_seq);
        Ok(self.as_available_write(producer_seq, barrier_seq - producer_seq))
    }

    /// Wait until at most `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait_upto(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        let (producer_seq, desired_seq) = self.batch_range(1);
        let barrier_seq = self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(barrier_seq > producer_seq);
        let batch_size = (barrier_seq - producer_seq).min(size as i64);
        Ok(self.as_available_write(producer_seq, batch_size))
    }
}

/// A handle with immutable access to events on the ring buffer.
///
/// Can access events concurrently to other handles with immutable access.
#[derive(Debug)]
pub struct Consumer<E, W> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W> Consumer<E, W> {
    /// Returns the current sequence value for this consumer's cursor.
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer.size()
    }

    #[inline]
    fn as_available_read(&mut self, sequence: i64, batch_size: i64) -> AvailableRead<'_, E> {
        AvailableRead {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: sequence,
            batch_size,
        }
    }
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer may take longer to become available.
    pub fn wait(&mut self, size: u32) -> AvailableRead<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + size as i64;
        self.wait_strategy.wait(batch_end, &self.barrier);
        debug_assert!(self.barrier.sequence() >= batch_end);
        self.as_available_read(consumer_seq, size as i64)
    }

    /// Wait until any number of events are available.
    pub fn wait_any(&mut self) -> AvailableRead<'_, E> {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.wait(consumer_seq + 1, &self.barrier);
        debug_assert!(barrier_seq > consumer_seq);
        self.as_available_read(consumer_seq, barrier_seq - consumer_seq)
    }

    /// Wait until at most `size` number of events are available.
    pub fn wait_upto(&mut self, size: u32) -> AvailableRead<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.wait(consumer_seq + 1, &self.barrier);
        debug_assert!(barrier_seq > consumer_seq);
        let batch_size = (barrier_seq - consumer_seq).min(size as i64);
        self.as_available_read(consumer_seq, batch_size)
    }
}

impl<E, W> Consumer<E, W>
where
    W: TryWaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer may take longer to become available.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableRead<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + size as i64;
        self.wait_strategy.try_wait(batch_end, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= batch_end);
        Ok(self.as_available_read(consumer_seq, size as i64))
    }

    /// Wait until any number of events are available.
    ///
    /// Otherwise, returns the wait strategy error.
    pub fn try_wait_any(&mut self) -> Result<AvailableRead<'_, E>, W::Error> {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.try_wait(consumer_seq + 1, &self.barrier)?;
        debug_assert!(barrier_seq > consumer_seq);
        Ok(self.as_available_read(consumer_seq, barrier_seq - consumer_seq))
    }

    /// Wait until at most `size` number of events are available.
    ///
    /// Otherwise, returns the wait strategy error.
    pub fn try_wait_upto(&mut self, size: u32) -> Result<AvailableRead<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.try_wait(consumer_seq + 1, &self.barrier)?;
        debug_assert!(barrier_seq > consumer_seq);
        let batch_size = (barrier_seq - consumer_seq).min(size as i64);
        Ok(self.as_available_read(consumer_seq, batch_size))
    }
}

// todo: elucidate docs with 'in another process' statements, eg. for describing mut alias possibility

/// Represents a range of available sequences which may be written to.
pub struct AvailableWrite<'a, E> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current: i64,
    batch_size: i64,
    move_cursor: fn(&Arc<Cursor>, i64, i64, Ordering),
}

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
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) Only one producer is accessing this sequence, so only one mutable ref will exist.
        // 2) Acquire-Release barrier ensures no other accesses to this sequence.
        unsafe {
            self.buffer.apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                let event: &mut E = &mut *ptr;
                write(event, seq, end);
            });
        }
        let seq_end = self.current + self.batch_size;
        (self.move_cursor)(self.cursor, self.current, seq_end, Ordering::Release);
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
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) Only one producer is accessing this sequence, so only one mutable ref will exist.
        // 2) Acquire-Release barrier ensures no other accesses to this sequence.
        unsafe {
            self.buffer.try_apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                let event: &mut E = &mut *ptr;
                write(event, seq, end)
            })?;
        }
        let seq_end = self.current + self.batch_size;
        (self.move_cursor)(self.cursor, self.current, seq_end, Ordering::Release);
        Ok(())
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
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) Only one producer is accessing this sequence, so only one mutable ref will exist.
        // 2) Acquire-Release barrier ensures no other accesses to this sequence.
        unsafe {
            self.buffer.try_apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                let event: &mut E = &mut *ptr;
                write(event, seq, end).map_err(|err| {
                    (self.move_cursor)(self.cursor, self.current, seq - 1, Ordering::Release);
                    err
                })
            })?;
        }
        let seq_end = self.current + self.batch_size;
        (self.move_cursor)(self.cursor, self.current, seq_end, Ordering::Release);
        Ok(())
    }
}

/// Represents a range of available sequences which may be read from.
pub struct AvailableRead<'a, E> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current: i64,
    batch_size: i64,
}

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
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) Only one producer is accessing this sequence, so only one mutable ref will exist.
        // 2) Acquire-Release barrier ensures no other accesses to this sequence.
        unsafe {
            self.buffer.apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                let event: &E = &*ptr;
                read(event, seq, end);
            });
        }
        let end_seq = self.current + self.batch_size;
        self.cursor.sequence.store(end_seq, Ordering::Release);
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
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) Only one producer is accessing this sequence, so only one mutable ref will exist.
        // 2) Acquire-Release barrier ensures no other accesses to this sequence.
        unsafe {
            self.buffer.try_apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                let event: &E = &*ptr;
                read(event, seq, end)
            })?;
        }
        let end_seq = self.current + self.batch_size;
        self.cursor.sequence.store(end_seq, Ordering::Release);
        Ok(())
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
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) Only one producer is accessing this sequence, so only one mutable ref will exist.
        // 2) Acquire-Release barrier ensures no other accesses to this sequence.
        unsafe {
            self.buffer.try_apply(self.current + 1, self.batch_size, |ptr, seq, end| {
                let event: &E = &*ptr;
                read(event, seq, end).map_err(|err| {
                    self.cursor.sequence.store(seq - 1, Ordering::Release);
                    err
                })
            })?;
        }
        let end_seq = self.current + self.batch_size;
        self.cursor.sequence.store(end_seq, Ordering::Release);
        Ok(())
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

    /// Create a cursor at the start of the sequence. All reads and writes begin on the _next_
    /// position in the sequence, thus cursors start at `-1`, so that reads and writes start at `0`.
    pub(crate) const fn start() -> Self {
        Cursor::new(-1)
    }
}

/// A collection of cursors that limits which sequence is available to a handle.
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
