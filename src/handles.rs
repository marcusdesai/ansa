use crate::ringbuffer::RingBuffer;
use crate::wait::{TryWaitStrategy, WaitStrategy};
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
    /// 2) The ring buffer size associated with this producer must be divisible by `BATCH`.
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

    #[inline]
    fn produce<F>(&mut self, current_claim: i64, claim_end: i64, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        for seq in current_claim + 1..=claim_end {
            // SAFETY:
            // 1) We know that this sequence has been claimed by only one producer, so multiple
            //    mutable refs to this element will not be created.
            // 2) Exclusive access to this sequence is ensured by the Acquire-Release barrier, so
            //    no other accesses of this data will be attempted.
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == claim_end);
        }
        // Busy wait for the cursor to catch up to the start of claimed sequence, ensuring buffer
        // writes are made visible in order. Without this check, unfinished writes before this
        // claim may become prematurely visible, allowing reads to overlap ongoing writes.
        while self.cursor.sequence.load(Ordering::Acquire) != current_claim {}
        // Publish writes upto the end of the claimed sequence.
        self.cursor.sequence.store(claim_end, Ordering::Release);
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// todo docs
    pub fn batch_write<F>(&mut self, size: u32, write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(size as usize <= self.buffer.len());
        let (current_claim, claim_end) = claim(&self.claim, size as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.len() as i64
        } else {
            claim_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(current_claim, claim_end, write);
    }
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// todo docs
    pub fn try_batch_write<F>(&mut self, size: u32, write: F) -> Result<(), W::Error>
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(size as usize <= self.buffer.len());
        let (current_claim, claim_end) = claim(&self.claim, size as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.len() as i64
        } else {
            claim_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(current_claim, claim_end, write);
        Ok(())
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

    #[inline]
    fn produce<F>(&mut self, current_claim: i64, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) We know that this sequence has been claimed by only one producer, so multiple
        //    mutable refs to this element will not be created.
        // 2) Exclusive access to this sequence is ensured by the Acquire-Release barrier, so
        //    no other accesses of this data will be attempted.
        // 3) The pointer is always guaranteed to be inbounds of the ring buffer allocation by the
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
        while self.cursor.sequence.load(Ordering::Acquire) != current_claim {}
        self.cursor.sequence.store(seq, Ordering::Release);
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactMultiProducer<E, W, LEAD, BATCH>
where
    W: WaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    /// busy waits when waiting for earlier claim to complete
    pub fn write_exact<F>(&mut self, write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        let (current_claim, claim_end) = claim(&self.claim, BATCH as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.len() as i64
        } else {
            claim_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(current_claim, write);
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactMultiProducer<E, W, LEAD, BATCH>
where
    W: TryWaitStrategy,
{
    /// todo docs
    pub fn try_write_exact<F>(&mut self, write: F) -> Result<(), W::Error>
    where
        F: FnMut(&mut E, i64, bool),
    {
        let (current_claim, claim_end) = claim(&self.claim, BATCH as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.len() as i64
        } else {
            claim_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(current_claim, write);
        Ok(())
    }
}

/// Returns a claim for a range of sequences.
///
/// The claimed range is exclusive to a single multi producer clone.
#[inline]
fn claim(claim: &Cursor, size: i64) -> (i64, i64) {
    let mut current_claim = claim.sequence.load(Ordering::Relaxed);
    let mut claim_end = current_claim + size;
    while let Err(new_current) = claim.sequence.compare_exchange(
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

/// todo docs
/// Writes events to the disruptor.
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
    /// 2) The ring buffer size associated with this producer must be divisible by `BATCH`.
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

    #[inline]
    fn produce<F>(&mut self, producer_seq: i64, batch_end: i64, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        for seq in producer_seq + 1..=batch_end {
            // SAFETY:
            // 1) We know that there is only one producer accessing this section of the buffer, so
            //    multiple mutable refs cannot exist.
            // 2) Exclusive access to this sequence is ensured by the Acquire-Release barrier, so
            //    no other accesses of this data will be attempted.
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == batch_end);
        }
        // Move cursor upto the end of the written batch.
        self.cursor.sequence.store(batch_end, Ordering::Release);
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: WaitStrategy,
{
    /// todo docs
    pub fn batch_write<F>(&mut self, size: u32, write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(size as usize <= self.buffer.len());
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + size as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.len() as i64
        } else {
            batch_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(producer_seq, batch_end, write);
    }
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: TryWaitStrategy,
{
    /// todo docs
    pub fn try_batch_write<F>(&mut self, size: u32, write: F) -> Result<(), W::Error>
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(size as usize <= self.buffer.len());
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + size as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.len() as i64
        } else {
            batch_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(producer_seq, batch_end, write);
        Ok(())
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

    #[inline]
    fn produce<F>(&mut self, producer_seq: i64, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) We know that there is only one producer accessing this section of the buffer, so
        //    multiple mutable refs cannot exist.
        // 2) Exclusive access to this sequence is ensured by the Acquire-Release barrier, so
        //    no other accesses of this data will be attempted.
        // 3) The pointer is always guaranteed to be inbounds of the ring buffer allocation by the
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
        self.cursor.sequence.store(seq, Ordering::Release);
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactProducer<E, W, LEAD, BATCH>
where
    W: WaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn write_exact<F>(&mut self, write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + BATCH as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.len() as i64
        } else {
            batch_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(producer_seq, write);
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactProducer<E, W, LEAD, BATCH>
where
    W: TryWaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn try_write_exact<F>(&mut self, write: F) -> Result<(), W::Error>
    where
        F: FnMut(&mut E, i64, bool),
    {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + BATCH as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.len() as i64
        } else {
            batch_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        assert!(self.barrier.sequence() >= desired_seq);
        self.produce(producer_seq, write);
        Ok(())
    }
}

/// Reads events from the disruptor.
///
/// A `Consumer` has only read access to events on the ring buffer, but as a consequence, multiple
/// `Consumer`s can make concurrent, overlapping accesses to the ring buffer.
///
/// `Consumer` provides fallible and non-fallible methods depending on which traits are implemented
/// by its wait strategy, `W`.
/// - If `W` implements [`TryWaitStrategy`], then fallible methods are provided.
/// - If `W` implements [`WaitStrategy`], then non-fallible methods are provided.
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
    /// Three conditions must be met to create an `ExactConsumer`:
    /// 1) `BATCH` must not be zero.
    /// 2) The ring buffer size associated with this consumer must be divisible by `BATCH`.
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

    #[inline]
    fn consume<F>(&self, consumer_seq: i64, batch_end: i64, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        fence(Ordering::Acquire);
        for seq in consumer_seq + 1..=batch_end {
            // SAFETY:
            // 1) The mutable pointer to the event is immediately converted to an immutable ref,
            //    ensuring multiple mutable refs are not created here.
            // 2) The Acquire-Release barrier ensures that following producers will not attempt
            //    writes to this sequence.
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == batch_end);
        }
        // Move cursor up to sequence at end of batch.
        self.cursor.sequence.store(batch_end, Ordering::Release);
    }
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategy,
{
    /// Read a batch of elements from the buffer.
    ///
    /// Waits until at least batch `size` number of elements are available,
    ///
    /// Strictly, `size` must be less than the size of the ring buffer, but practically it must be
    /// smaller than that. Very large batch sizes will cause handles to bunch up and stall while
    /// waiting for large portions of the buffer to become available.
    ///
    /// `read` is a callback with the signature:
    /// `read(event: &E, sequence: i64, batch_end: bool)`
    ///
    /// - `event` is the event being read from the buffer.
    /// - `sequence` is the sequence number at which this event is read.
    /// - `batch_end` indicates whether this event is the last in the requested batch.
    ///
    /// The logic in `read` should not panic, as doing so will cause the `Consumer` to stop and
    /// become unrecoverable. If any handle in the disruptor permanently stops, the entire
    /// disruptor may eventually permanently stall.
    ///
    /// The logic of this method, as provided by `ansa`, is guaranteed not to panic. Please report
    /// an issue if this method panics due to the library implementation.
    ///
    /// # Examples
    /// ```no_run
    /// use ansa::*;
    ///
    /// let event = 0i64;
    ///
    /// let mut handles = DisruptorBuilder::new(64, || event)
    ///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
    ///     .build()
    ///     .unwrap();
    ///
    /// let consumer = handles.take_consumer(0).unwrap();
    ///
    /// fn read(event: &i64, sequence: i64, batch_end: bool) {
    ///     // do something
    /// }
    ///
    /// consumer.batch_read(8, read);
    ///
    /// // or using a closure
    /// consumer.batch_read(8, |event, seq, end| {
    ///     // do something
    /// });
    /// ```
    /// The code above is contrived and meant only to show straightforwardly how the API can be
    /// used. For more representative usage examples, see the top level [`ansa`](crate) docs.
    pub fn batch_read<F>(&self, size: u32, read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        assert!(size as usize <= self.buffer.len());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + size as i64;
        self.wait_strategy.wait(batch_end, &self.barrier);
        assert!(self.barrier.sequence() >= batch_end);
        self.consume(consumer_seq, batch_end, read)
    }

    /// Specialised function which will always consume *all* available buffer elements when called.
    ///
    /// Can flexibly read as many events as are available to consume. Waits until there is at least
    /// one event to consume.
    ///
    /// See [`batch_read`](Consumer::batch_read) for further documentation.
    pub fn read<F>(&self, read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.wait(consumer_seq + 1, &self.barrier);
        assert!(barrier_seq > consumer_seq);
        self.consume(consumer_seq, barrier_seq, read)
    }
}

impl<E, W> Consumer<E, W>
where
    W: TryWaitStrategy,
{
    /// todo docs
    pub fn try_batch_read<F>(&self, size: u32, read: F) -> Result<(), W::Error>
    where
        F: FnMut(&E, i64, bool),
    {
        assert!(size as usize <= self.buffer.len());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + size as i64;
        self.wait_strategy.try_wait(batch_end, &self.barrier)?;
        assert!(self.barrier.sequence() >= batch_end);
        self.consume(consumer_seq, batch_end, read);
        Ok(())
    }

    /// todo docs
    pub fn try_read<F>(&self, read: F) -> Result<(), W::Error>
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.try_wait(consumer_seq + 1, &self.barrier)?;
        assert!(barrier_seq > consumer_seq);
        self.consume(consumer_seq, barrier_seq, read);
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

    #[inline]
    fn consume<F>(&self, consumer_seq: i64, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) The mutable pointer to the event is immediately converted to an immutable pointer,
        //    ensuring multiple mutable refs cannot be created here.
        // 2) The Acquire-Release barrier ensures that following producers will not attempt
        //    writes to this sequence.
        // 3) The pointer is always guaranteed to be inbounds of the ring buffer allocation by the
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
        self.cursor.sequence.store(seq, Ordering::Release);
    }
}

impl<E, W, const BATCH: u32> ExactConsumer<E, W, BATCH>
where
    W: WaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn read_exact<F>(&self, read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        self.wait_strategy.wait(consumer_seq + BATCH as i64, &self.barrier);
        assert!(self.barrier.sequence() >= consumer_seq + BATCH as i64);
        self.consume(consumer_seq, read);
    }
}

impl<E, W, const BATCH: u32> ExactConsumer<E, W, BATCH>
where
    W: TryWaitStrategy,
{
    /// todo docs
    /// Works with Tree-Borrows but not Stacked-Borrows
    pub fn try_read_exact<F>(&self, read: F) -> Result<(), W::Error>
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        self.wait_strategy.try_wait(consumer_seq + BATCH as i64, &self.barrier)?;
        assert!(self.barrier.sequence() >= consumer_seq + BATCH as i64);
        self.consume(consumer_seq, read);
        Ok(())
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
        assert_eq!(size_of::<ExactConsumer<u8, WaitBusy, 8>>(), 32);
        assert_eq!(size_of::<Producer<u8, WaitBusy, true>>(), 32);
        assert_eq!(size_of::<ExactProducer<u8, WaitBusy, true, 8>>(), 32);
        assert_eq!(size_of::<MultiProducer<u8, WaitBusy, true>>(), 40);
        assert_eq!(size_of::<ExactMultiProducer<u8, WaitBusy, true, 8>>(), 40);
    }
}
