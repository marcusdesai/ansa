use crate::ringbuffer::RingBuffer;
use crate::wait::{TimedOut, WaitStrategy, WaitStrategyTimeout};
use std::sync::atomic::{fence, AtomicI64, Ordering};
use std::sync::Arc;

/// todo docs
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
    /// Returns the count of [`MultiProducer`] associated with this producer's cursor.
    ///
    /// Care should be taken when performing actions based upon this number, as any thread which
    /// holds an associated [`MultiProducer`] may clone it at any time, thereby changing the count.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let mut handles = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()
    ///     .unwrap();
    ///
    /// let multi = handles.take_lead().unwrap().into_multi();
    /// assert_eq!(multi.count(), 1);
    /// let multi_2 = multi.clone();
    /// assert_eq!(multi.count(), 2);
    /// // consume a `MultiProducer` by attempting the conversion into a `Producer`
    /// assert!(matches!(multi.into_single(), None));
    /// assert_eq!(multi_2.count(), 1);
    /// ```
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.claim)
    }

    /// Return a [`Producer`] if there exists only one [`MultiProducer`] associated with
    /// this producer cursor.
    ///
    /// Otherwise, return `None` and drop this [`MultiProducer`].
    ///
    /// If this function is called when only one [`MultiProducer`] exists, then it is guaranteed to
    /// return a [`Producer`]. Note that independent [`MultiProducer`]s, which do not share a
    /// cursor, will not affect calls to this method.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let mut handles = DisruptorBuilder::new(16, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build();
    ///
    /// let multi = handles.unwrap().take_lead().unwrap().into_multi();
    /// let multi_clone = multi.clone();
    ///
    /// assert!(matches!(multi.into_single(), None));
    /// assert!(matches!(multi_clone.into_single(), Some(Producer { .. })));
    /// ```
    pub fn into_single(self) -> Option<Producer<E, W, LEAD>> {
        Arc::into_inner(self.claim).map(|_| Producer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }

    /// Attempts to create an [`ExactMultiProducer`] from this [`MultiProducer`].
    ///
    /// Three conditions must be met to create an [`ExactMultiProducer`]:
    /// 1) `BATCH` must not be zero.
    /// 2) The buffer size associated with this producer must be divisible by `BATCH`.
    /// 3) This producer cursor's sequence value + 1 must be divisible by `BATCH`. Bear in mind
    ///    that sequence values start at `-1`.
    ///
    /// Note that, before this producer writes any data to the buffer (i.e., moves its cursor),
    /// the third condition is trivially met by any `BATCH` value.
    ///
    /// If any of these conditions are _not_ met, returns `Err(`[`MultiProducer`]`)`.
    ///
    /// If these conditions are met, but more than one [`MultiProducer`] exists for this producer
    /// cursor, returns `Ok(None)`, and consumes the calling producer. This behaviour ensures
    /// exact and non-exact multi producers won't exist for the same cursor. If allowed, then exact
    /// producers would lose the guarantee to not perform out-of-bounds ring buffer accesses.
    ///
    /// Otherwise, returns `Ok(Some(`[`ExactMultiProducer`]`))`.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let buffer_size = 64;
    /// let mut handles = DisruptorBuilder::new(buffer_size, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()
    ///     .unwrap();
    ///
    /// let multi = handles.take_lead().unwrap().into_multi();
    /// let multi_2 = multi.clone();
    ///
    /// // two multi producers exist for this cursor, but the invalid BATCH value means
    /// // neither will be consumed in this call
    /// let result = multi.into_exact::<10>();
    /// assert!(matches!(result, Err(MultiProducer { .. })));
    ///
    /// // BATCH meets the required conditions, but there are two producers for this cursor
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(None)));
    ///
    /// let result = multi_2.into_exact::<16>();
    /// assert!(matches!(result, Ok(Some(ExactMultiProducer { .. }))));
    /// ```
    pub fn into_exact<const BATCH: u32>(
        self,
    ) -> Result<Option<ExactMultiProducer<E, W, LEAD, BATCH>>, Self> {
        if BATCH == 0 || self.buffer.len() % BATCH as usize != 0 {
            return Err(self);
        }
        match Arc::into_inner(self.claim) {
            None => Ok(None),
            Some(claim) => {
                let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
                if sequence % BATCH as i64 != 0 {
                    // There is at most one producer associated with this cursor, so a new shared
                    // claim can be made without issue.
                    return Err(MultiProducer {
                        cursor: self.cursor,
                        barrier: self.barrier,
                        buffer: self.buffer,
                        claim: Arc::new(claim),
                        wait_strategy: self.wait_strategy,
                    });
                }
                Ok(Some(ExactMultiProducer {
                    cursor: self.cursor,
                    barrier: self.barrier,
                    buffer: self.buffer,
                    claim: Arc::new(claim),
                    wait_strategy: self.wait_strategy,
                }))
            }
        }
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// todo docs
    pub fn batch_write<F>(&mut self, size: u32, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(
            (size as usize) <= self.buffer.len(),
            "batch size ({size}) > buffer len ({})",
            self.buffer.len()
        );
        let size = size as i64;
        // Claim a sequence. The 'claim' is used to coordinate the multi producer clones.
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
        // Wait for the barrier to move past the end of the claim.
        let expected = if LEAD {
            claim_end - self.buffer.len() as i64
        } else {
            claim_end
        };
        self.wait_strategy.wait(expected, &self.barrier);
        assert!(self.barrier.sequence() >= expected);
        // Ensure synchronisation occurs by creating an Acquire-Release barrier. This needs to
        // cover the entire duration of the buffer writes, so we use a fence here before any of the
        // cursor loads.
        fence(Ordering::Acquire);
        // Begin writing events to the buffer.
        for seq in current_claim + 1..=claim_end {
            // SAFETY:
            // 1) We know that this sequence has been claimed by only one producer, so multiple
            //    mutable refs to this element cannot exist.
            // 2) The Acquire-Release memory barrier ensures this memory location will not be read
            //    while it is written to. This ensures that immutable refs will not be created for
            //    this element while the mutable ref exists.
            // 3) The pointer being dereferenced is guaranteed to point at correctly initialised
            //    and aligned memory, because the ring buffer is pre-allocated and pre-populated.
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == claim_end);
        }
        // Wait for the cursor to catch up to start of claimed sequence. This ensures that writes
        // later in the sequence are not made visible until all earlier writes by this multi
        // producer or its clones are completed. Without this check, unfinished writes may become
        // prematurely visible, allowing overlapping immutable and mutable refs to be created.
        let mut cursor_seq = self.cursor.sequence.load(Ordering::Acquire);
        while cursor_seq != current_claim {
            cursor_seq = self.cursor.sequence.load(Ordering::Acquire);
        }
        // Finally, advance producer cursor to publish the writes upto the end of the claimed
        // sequence.
        self.cursor.sequence.store(claim_end, Ordering::Release);
    }
}

/// todo docs
/// Construction conditions ensure no out-of-bounds buffer accesses, allowing these accesses to be
/// optimised.
#[derive(Debug)]
pub struct ExactMultiProducer<E, W, const LEAD: bool, const BATCH: u32> {
    cursor: Arc<Cursor>,
    barrier: Barrier,
    buffer: Arc<RingBuffer<E>>,
    claim: Arc<Cursor>, // shared between all clones of this multi producer
    wait_strategy: W,
}

impl<E, W, const LEAD: bool, const BATCH: u32> Clone for ExactMultiProducer<E, W, LEAD, BATCH>
where
    W: Clone,
{
    fn clone(&self) -> Self {
        ExactMultiProducer {
            cursor: Arc::clone(&self.cursor),
            barrier: self.barrier.clone(),
            buffer: Arc::clone(&self.buffer),
            claim: Arc::clone(&self.claim),
            wait_strategy: self.wait_strategy.clone(),
        }
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactMultiProducer<E, W, LEAD, BATCH> {
    /// Returns the count of [`ExactMultiProducer`] associated with this producer's cursor.
    ///
    /// Care should be taken when performing actions based upon this number, as any thread which
    /// holds an associated [`ExactMultiProducer`] may clone it at any time, thereby changing the
    /// count.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let mut handles = DisruptorBuilder::new(64, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()
    ///     .unwrap();
    ///
    /// let lead = handles.take_lead().unwrap().into_multi();
    /// let exact_multi = lead.into_exact::<16>().expect("exact").unwrap();
    ///
    /// assert_eq!(exact_multi.count(), 1);
    /// let exact_multi_2 = exact_multi.clone();
    /// assert_eq!(exact_multi.count(), 2);
    /// // consume `ExactMultiProducer` by attempting conversion into `MultiProducer`
    /// assert!(matches!(exact_multi.into_multi(), None));
    /// assert_eq!(exact_multi_2.count(), 1);
    /// ```
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.claim)
    }

    /// todo docs
    pub fn into_multi(self) -> Option<MultiProducer<E, W, LEAD>> {
        Arc::into_inner(self.claim).map(|claim| MultiProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            claim: Arc::new(claim),
            wait_strategy: self.wait_strategy,
        })
    }

    /// See docs for [`MultiProducer::into_exact`].
    pub fn into_exact<const NEW: u32>(
        self,
    ) -> Result<Option<ExactMultiProducer<E, W, LEAD, NEW>>, Self> {
        if NEW == 0 || self.buffer.len() % NEW as usize != 0 {
            return Err(self);
        }
        match Arc::into_inner(self.claim) {
            None => Ok(None),
            Some(claim) => {
                let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
                if sequence % NEW as i64 != 0 {
                    // There is at most one producer associated with this cursor, so a new shared
                    // claim can be made without issue.
                    return Err(ExactMultiProducer {
                        cursor: self.cursor,
                        barrier: self.barrier,
                        buffer: self.buffer,
                        claim: Arc::new(claim),
                        wait_strategy: self.wait_strategy,
                    });
                }
                Ok(Some(ExactMultiProducer {
                    cursor: self.cursor,
                    barrier: self.barrier,
                    buffer: self.buffer,
                    claim: Arc::new(claim),
                    wait_strategy: self.wait_strategy,
                }))
            }
        }
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactMultiProducer<E, W, LEAD, BATCH>
where
    W: WaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn write_exact<F>(&mut self, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        let mut current_claim = self.claim.sequence.load(Ordering::Relaxed);
        let mut claim_end = current_claim + BATCH as i64;
        while let Err(new_current) = self.claim.sequence.compare_exchange(
            current_claim,
            claim_end,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            current_claim = new_current;
            claim_end = new_current + BATCH as i64;
        }
        let expected = if LEAD {
            claim_end - self.buffer.len() as i64
        } else {
            claim_end
        };
        self.wait_strategy.wait(expected, &self.barrier);
        assert!(self.barrier.sequence() >= expected);
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) We know that this sequence has been claimed by only one producer, so multiple
        //    mutable refs to this element cannot exist.
        // 2) The Acquire-Release memory barrier ensures this memory location will not be read
        //    while it is written to. This ensures that immutable refs will not be created for
        //    this element while the mutable ref exists.
        // 3) The pointer being dereferenced is guaranteed to point at correctly initialised
        //    and aligned memory, because the ring buffer is pre-allocated and pre-populated.
        // 4) The pointer is always guaranteed to be inbounds of the ring buffer allocation by the
        //    checks on BATCH size made when creating this struct.
        let mut seq = current_claim + 1;
        let mut pointer = self.buffer.get(seq);
        unsafe {
            for _ in 0..BATCH - 1 {
                write(&mut *pointer, seq, false);
                pointer = pointer.add(1);
                seq += 1;
            }
            write(&mut *pointer, seq, true);
        }
        let mut cursor_seq = self.cursor.sequence.load(Ordering::Acquire);
        while cursor_seq != current_claim {
            cursor_seq = self.cursor.sequence.load(Ordering::Acquire);
        }
        assert_eq!(seq, claim_end);
        self.cursor.sequence.store(seq, Ordering::Release);
    }
}

/// todo docs
#[derive(Debug)]
pub struct Producer<E, W, const LEAD: bool> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD> {
    /// todo docs
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

    /// Returns an [`ExactProducer`] if successful, otherwise returns the producer which called
    /// this method.
    ///
    /// Three conditions must be met to create an [`ExactProducer`]:
    /// 1) `BATCH` must not be zero.
    /// 2) The buffer size associated with this producer must be divisible by `BATCH`.
    /// 3) This producer cursor's sequence value + 1 must be divisible by `BATCH`. Bear in mind
    ///    that sequence values start at `-1`.
    ///
    /// Note that, before this producer writes any data to the buffer (i.e., moves its cursor),
    /// the third condition is trivially met by any `BATCH` value.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let buffer_size = 64;
    /// let mut handles = DisruptorBuilder::new(buffer_size, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()
    ///     .unwrap();
    ///
    /// let producer = handles.take_lead().unwrap();
    ///
    /// let result = producer.into_exact::<10>();
    /// assert!(matches!(result, Err(Producer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(ExactProducer { .. })));
    /// ```
    pub fn into_exact<const BATCH: u32>(self) -> Result<ExactProducer<E, W, LEAD, BATCH>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(BATCH, self.buffer.len(), sequence) {
            return Err(self);
        }
        Ok(ExactProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// todo docs
    pub fn batch_write<F>(&mut self, size: u32, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(
            (size as usize) <= self.buffer.len(),
            "batch size ({size}) > buffer len ({})",
            self.buffer.len()
        );
        let size = size as i64;
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        // Wait until there are free slots to write events to.
        let expected = if LEAD {
            producer_seq + size - self.buffer.len() as i64
        } else {
            producer_seq + size
        };
        self.wait_strategy.wait(expected, &self.barrier);
        assert!(self.barrier.sequence() >= expected);
        // Ensure synchronisation occurs by creating an Acquire-Release barrier for the entire
        // duration of the writes.
        fence(Ordering::Acquire);
        // Begin writing events to the buffer.
        let batch_end = producer_seq + size;
        for seq in producer_seq + 1..=batch_end {
            // SAFETY:
            // 1) We know that there is only one producer accessing this section of the buffer, so
            //    multiple mutable refs cannot exist.
            // 2) The Acquire-Release memory barrier ensures this memory location will not be read
            //    while it is written to. This ensures that immutable refs will not be created for
            //    this element while the mutable ref exists.
            // 3) The pointer being dereferenced is guaranteed to point at correctly initialised
            //    and aligned memory, because the ring buffer is pre-allocated and pre-populated.
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == batch_end);
        }
        // Move cursor upto the end of the written batch.
        self.cursor.sequence.store(batch_end, Ordering::Release);
    }
}

/// todo docs
#[derive(Debug)]
pub struct ExactProducer<E, W, const LEAD: bool, const BATCH: u32> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactProducer<E, W, LEAD, BATCH> {
    /// Converts this [`ExactProducer`] into a [`Producer`].
    pub fn into_producer(self) -> Producer<E, W, LEAD> {
        Producer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        }
    }

    /// See docs for [`Producer::into_exact`].
    pub fn into_exact<const NEW: u32>(self) -> Result<ExactProducer<E, W, LEAD, NEW>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(NEW, self.buffer.len(), sequence) {
            return Err(self);
        }
        Ok(ExactProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactProducer<E, W, LEAD, BATCH>
where
    W: WaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn write_exact<F>(&mut self, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let expected = if LEAD {
            producer_seq + BATCH as i64 - self.buffer.len() as i64
        } else {
            producer_seq + BATCH as i64
        };
        self.wait_strategy.wait(expected, &self.barrier);
        assert!(self.barrier.sequence() >= expected);
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) We know that there is only one producer accessing this section of the buffer, so
        //    multiple mutable refs cannot exist.
        // 2) The Acquire-Release memory barrier ensures this memory location will not be read
        //    while it is written to. This ensures that immutable refs will not be created for
        //    this element while the mutable ref exists.
        // 3) The pointer being dereferenced is guaranteed to point at correctly initialised
        //    and aligned memory, because the ring buffer is pre-allocated and pre-populated.
        // 4) The pointer is always guaranteed to be inbounds of the ring buffer allocation by the
        //    checks on BATCH size made when creating this struct.
        let mut seq = producer_seq + 1;
        let mut pointer = self.buffer.get(seq);
        unsafe {
            for _ in 0..BATCH - 1 {
                write(&mut *pointer, seq, false);
                pointer = pointer.add(1);
                seq += 1;
            }
            write(&mut *pointer, seq, true);
        }
        assert_eq!(seq, producer_seq + BATCH as i64);
        self.cursor.sequence.store(seq, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct Consumer<E, W> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W> Consumer<E, W> {
    /// Returns an [`ExactConsumer`] if successful, otherwise returns the consumer which called
    /// this method.
    ///
    /// Three conditions must be met to create an [`ExactConsumer`]:
    /// 1) `BATCH` must not be zero.
    /// 2) The buffer size associated with this consumer must be divisible by `BATCH`.
    /// 3) This consumer cursor's sequence value + 1 must be divisible by `BATCH`. Bear in mind
    ///    that sequence values start at `-1`.
    ///
    /// Note that, before this consumer reads any data from the buffer (i.e., moves its cursor),
    /// the third condition is trivially met by any `BATCH` value.
    ///
    /// # Examples
    /// ```
    /// use ansa::*;
    ///
    /// let buffer_size = 64;
    /// let mut handles = DisruptorBuilder::new(buffer_size, || 0)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()
    ///     .unwrap();
    ///
    /// let consumer = handles.take_consumer(0).unwrap();
    ///
    /// let result = consumer.into_exact::<10>();
    /// assert!(matches!(result, Err(Consumer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(ExactConsumer { .. })));
    /// ```
    pub fn into_exact<const BATCH: u32>(self) -> Result<ExactConsumer<E, W, BATCH>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(BATCH, self.buffer.len(), sequence) {
            return Err(self);
        }
        Ok(ExactConsumer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategy,
{
    /// todo docs
    pub fn batch_read<F>(&self, size: u32, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        assert!(
            (size as usize) <= self.buffer.len(),
            "batch size ({size}) > buffer len ({})",
            self.buffer.len()
        );
        let size = size as i64;
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        // Wait until there are events to read.
        self.wait_strategy.wait(consumer_seq + size, &self.barrier);
        assert!(self.barrier.sequence() >= consumer_seq + size);
        // Ensure synchronisation occurs by creating an Acquire-Release barrier for the entire
        // duration of the reads.
        fence(Ordering::Acquire);
        // Begin reading a batch of events from the buffer.
        let batch_end = consumer_seq + size;
        for seq in consumer_seq + 1..=batch_end {
            // SAFETY:
            // 1) The mutable pointer to the event is immediately converted to an immutable ref,
            //    ensuring multiple mutable refs do not exist.
            // 2) The Acquire-Release synchronisation ensures that the consumer cursor does not
            //    visibly update its value until all the events are processed, which in turn ensures
            //    that producers will not write here while the consumer is reading. This ensures
            //    that no mutable ref to this element is created while this immutable ref exists.
            // 3) The pointer being dereferenced is guaranteed to point at correctly initialised
            //    and aligned memory, because the ring buffer is pre-allocated and pre-populated.
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == batch_end);
        }
        // Move cursor up to barrier sequence.
        self.cursor.sequence.store(batch_end, Ordering::Release);
    }

    /// Specialised function which will always consume *all* available buffer elements when called.
    ///
    /// See [`batch_read`](Consumer::batch_read) for further documentation.
    pub fn read<F>(&self, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.wait(consumer_seq + 1, &self.barrier);
        assert!(barrier_seq > consumer_seq);
        fence(Ordering::Acquire);
        for seq in consumer_seq + 1..=barrier_seq {
            // SAFETY: see Consumer::batch_read
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == barrier_seq);
        }
        self.cursor.sequence.store(barrier_seq, Ordering::Release);
    }
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategyTimeout,
{
    /// todo docs
    pub fn read_timeout<F>(&self, mut read: F) -> Result<(), TimedOut>
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.wait_timeout(consumer_seq + 1, &self.barrier)?;
        assert!(barrier_seq > consumer_seq);
        fence(Ordering::Acquire);
        for seq in consumer_seq + 1..=barrier_seq {
            // SAFETY: see Consumer::batch_read
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == barrier_seq);
        }
        self.cursor.sequence.store(barrier_seq, Ordering::Release);
        Ok(())
    }
}

/// todo docs
#[derive(Debug)]
pub struct ExactConsumer<E, W, const BATCH: u32> {
    cursor: Arc<Cursor>,
    barrier: Barrier,
    buffer: Arc<RingBuffer<E>>,
    wait_strategy: W,
}

impl<E, W, const BATCH: u32> ExactConsumer<E, W, BATCH> {
    /// Converts this [`ExactConsumer`] into a [`Consumer`].
    pub fn into_consumer(self) -> Consumer<E, W> {
        Consumer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        }
    }

    /// See docs for [`Consumer::into_exact`].
    pub fn into_exact<const NEW: u32>(self) -> Result<ExactConsumer<E, W, NEW>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(NEW, self.buffer.len(), sequence) {
            return Err(self);
        }
        Ok(ExactConsumer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }
}

impl<E, W, const BATCH: u32> ExactConsumer<E, W, BATCH>
where
    W: WaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn read_exact<F>(&self, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        self.wait_strategy.wait(consumer_seq + BATCH as i64, &self.barrier);
        assert!(self.barrier.sequence() >= consumer_seq + BATCH as i64);
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) The mutable pointer to the event is immediately converted to an immutable pointer,
        //    ensuring multiple mutable refs cannot be created here.
        // 2) The Acquire-Release synchronisation ensures that the consumer cursor does not visibly
        //    update its value until all the events are processed, which in turn ensures that
        //    producers will not write here while the consumer is reading. This ensures that no
        //    mutable ref to this element is created while this immutable ref exists.
        // 3) The pointer being dereferenced is guaranteed to point at correctly initialised
        //    and aligned memory, because the ring buffer is pre-allocated and pre-populated.
        // 4) The pointer is always guaranteed to be inbounds of the ring buffer allocation by the
        //    checks on BATCH size made when creating this struct.
        let mut seq = consumer_seq + 1;
        let mut pointer = self.buffer.get(seq) as *const E;
        unsafe {
            for _ in 0..BATCH - 1 {
                read(&*pointer, seq, false);
                pointer = pointer.add(1);
                seq += 1;
            }
            read(&*pointer, seq, true);
        }
        assert_eq!(seq, consumer_seq + BATCH as i64);
        self.cursor.sequence.store(seq, Ordering::Release);
    }
}

#[inline]
fn invalid_batch_size(batch: u32, buffer_size: usize, sequence: i64) -> bool {
    batch == 0 || buffer_size % batch as usize != 0 || sequence % batch as i64 != 0
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
    /// position in the sequence, so cursors start at `-1`, meaning reads and writes start at `0`.
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

    #[inline]
    pub(crate) fn sequence(&self) -> i64 {
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
