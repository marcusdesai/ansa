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
    /// assert!(matches!(multi.into_single(), None));
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
    /// assert!(matches!(multi.into_single(), None));
    /// assert!(matches!(multi_clone.into_single(), Some(ansa::Producer { .. })));
    /// ```
    pub fn into_single(self) -> Option<Producer<E, W, LEAD>> {
        Arc::into_inner(self.claim).map(|_| Producer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        })
    }

    /// Returns an [`ExactMultiProducer`] if `BATCH` is valid and exactly one [`MultiProducer`]
    /// exists for this cursor.
    ///
    /// Valid `BATCH` values must meet the following conditions:
    /// 1) `BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `BATCH`.
    ///
    /// If `BATCH` is invalid, returns the calling producer.
    ///
    /// If `BATCH` is valid, but more than one [`MultiProducer`] exists for this cursor, returns
    /// `Ok(None)` and the caller is dropped.
    ///
    /// # Examples
    /// ```
    /// use ansa::{mpsc, ExactMultiProducer, MultiProducer};
    ///
    /// let buffer_size = 64;
    /// let (multi, _) = mpsc(buffer_size, || 0);
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
        if BATCH == 0 || self.buffer.size() % BATCH as usize != 0 {
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
    /// Wait until at least `size` number of events are available.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer take longer to become available.
    pub fn wait(&mut self, size: u32) -> AvailableWrite<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let (current_claim, claim_end) = claim(&self.claim, size as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.size() as i64
        } else {
            claim_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= desired_seq);
        AvailableWrite {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: current_claim,
            end: claim_end,
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
    W: TryWaitStrategy,
{
    /// Wait until at least `size` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer take longer to become available.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let (current_claim, claim_end) = claim(&self.claim, size as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.size() as i64
        } else {
            claim_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= desired_seq);
        Ok(AvailableWrite {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: current_claim,
            end: claim_end,
            move_cursor: |cursor, current, end, ordering| {
                // Busy wait for the cursor to catch up to the start of claimed sequence.
                while cursor.sequence.load(Ordering::Acquire) != current {}
                cursor.sequence.store(end, ordering)
            },
        })
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
    /// Returns the count of [`ExactMultiProducer`]s associated with this producer's cursor.
    ///
    /// Care should be taken when performing actions based upon this number, as any thread which
    /// holds an associated [`ExactMultiProducer`] may clone it at any time, thereby changing the
    /// count.
    ///
    /// # Examples
    /// ```
    /// let (multi, _) = ansa::mpsc(64, || 0);
    /// let exact_multi = multi.into_exact::<16>().expect("exact").unwrap();
    /// assert_eq!(exact_multi.count(), 1);
    ///
    /// let exact_multi_2 = exact_multi.clone();
    /// assert_eq!(exact_multi.count(), 2);
    ///
    /// // consume `ExactMultiProducer` by attempting conversion into `MultiProducer`
    /// assert!(matches!(exact_multi.into_multi(), None));
    /// assert_eq!(exact_multi_2.count(), 1);
    /// ```
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.claim)
    }

    /// Returns the current sequence value for this producer's cursor.
    ///
    /// **Important:** The cursor for a `ExactMultiProducer` is shared by all of its clones. Any of
    /// these clones could alter this value at any time, if they are writing to the buffer.
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer.size()
    }

    /// Return a [`MultiProducer`] if exactly one [`ExactMultiProducer`] exists for this cursor.
    ///
    /// Otherwise, return `None` and drops this producer.
    ///
    /// If this function is called when only one `ExactMultiProducer` exists, then it is
    /// guaranteed to return a `MultiProducer`.
    ///
    /// # Examples
    /// ```
    /// let (producer, _) = ansa::spsc(64, || 0);
    /// let exact_multi = producer.into_exact_multi::<32>().unwrap();
    /// let exact_clone = exact_multi.clone();
    ///
    /// assert!(matches!(exact_multi.into_multi(), None));
    /// assert!(matches!(exact_clone.into_multi(), Some(ansa::MultiProducer { .. })));
    /// ```
    pub fn into_multi(self) -> Option<MultiProducer<E, W, LEAD>> {
        Arc::into_inner(self.claim).map(|claim| MultiProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            claim: Arc::new(claim),
            wait_strategy: self.wait_strategy,
        })
    }

    /// Returns a new [`ExactMultiProducer`] if `NEW_BATCH` is valid and exactly one
    /// `ExactMultiProducer` exists for this cursor.
    ///
    /// Valid `NEW_BATCH` values must meet the following conditions:
    /// 1) `NEW_BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `NEW_BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `NEW_BATCH`.
    ///
    /// If `NEW_BATCH` is invalid, returns the calling producer.
    ///
    /// If `NEW_BATCH` is valid, but more than one `ExactMultiProducer` exists for this cursor,
    /// returns `Ok(None)` and the caller is dropped.
    ///
    /// # Examples
    /// ```
    /// let buffer_size = 64;
    /// let (producer, _) = ansa::spsc(64, || 0);
    ///
    /// let exact_multi = producer.into_exact_multi::<16>().unwrap();
    /// let exact_clone = exact_multi.clone();
    ///
    /// // two multi producers exist for this cursor, but the invalid BATCH value means
    /// // neither will be consumed in this call
    /// let result = exact_multi.into_exact::<10>();
    /// assert!(matches!(result, Err(ansa::ExactMultiProducer { .. })));
    ///
    /// // BATCH meets the required conditions, but there are two producers for this cursor
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(None)));
    ///
    /// let result = exact_clone.into_exact::<16>();
    /// assert!(matches!(result, Ok(Some(ansa::ExactMultiProducer { .. }))));
    /// ```
    pub fn into_exact<const NEW_BATCH: u32>(
        self,
    ) -> Result<Option<ExactMultiProducer<E, W, LEAD, NEW_BATCH>>, Self> {
        if NEW_BATCH == 0 || self.buffer.size() % NEW_BATCH as usize != 0 {
            return Err(self);
        }
        match Arc::into_inner(self.claim) {
            None => Ok(None),
            Some(claim) => {
                let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
                if sequence % NEW_BATCH as i64 != 0 {
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
    /// Wait until at least `BATCH` number of events are available.
    pub fn wait(&mut self) -> AvailableWriteExact<'_, E, BATCH> {
        let (current_claim, claim_end) = claim(&self.claim, BATCH as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.size() as i64
        } else {
            claim_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= desired_seq);
        AvailableWriteExact {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: current_claim,
            move_cursor: |cursor, current, end, ordering| {
                // Busy wait for the cursor to catch up to the start of claimed sequence.
                while cursor.sequence.load(Ordering::Acquire) != current {}
                cursor.sequence.store(end, ordering)
            },
        }
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactMultiProducer<E, W, LEAD, BATCH>
where
    W: TryWaitStrategy,
{
    /// Wait until at least `BATCH` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait(&mut self) -> Result<AvailableWriteExact<'_, E, BATCH>, W::Error> {
        let (current_claim, claim_end) = claim(&self.claim, BATCH as i64);
        let desired_seq = if LEAD {
            claim_end - self.buffer.size() as i64
        } else {
            claim_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= desired_seq);
        Ok(AvailableWriteExact {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: current_claim,
            move_cursor: |cursor, current, end, ordering| {
                // Busy wait for the cursor to catch up to the start of claimed sequence.
                while cursor.sequence.load(Ordering::Acquire) != current {}
                cursor.sequence.store(end, ordering)
            },
        })
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

    /// Returns an [`ExactMultiProducer`] if `BATCH` is valid.
    ///
    /// Otherwise, returns the calling producer.
    ///
    /// Valid `BATCH` values must meet the following conditions:
    /// 1) `BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `BATCH`.
    ///
    /// # Examples
    /// ```
    /// use ansa::{spsc, ExactMultiProducer, Producer};
    ///
    /// let buffer_size = 64;
    /// let (producer, _) = spsc(buffer_size, || 0);
    ///
    /// let result = producer.into_exact_multi::<10>();
    /// assert!(matches!(result, Err(Producer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact_multi::<16>();
    /// assert!(matches!(result, Ok(ExactMultiProducer { .. })));
    /// ```
    pub fn into_exact_multi<const BATCH: u32>(
        self,
    ) -> Result<ExactMultiProducer<E, W, LEAD, BATCH>, Self> {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(BATCH, self.buffer.size(), producer_seq) {
            return Err(self);
        }
        Ok(ExactMultiProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            claim: Arc::new(Cursor::new(producer_seq - 1)),
            wait_strategy: self.wait_strategy,
        })
    }

    /// Returns an [`ExactProducer`] if `BATCH` is valid.
    ///
    /// Otherwise, returns the calling producer.
    ///
    /// Valid `BATCH` values must meet the following conditions:
    /// 1) `BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `BATCH`.
    ///
    /// # Examples
    /// ```
    /// use ansa::{spsc, ExactProducer, Producer};
    ///
    /// let buffer_size = 64;
    /// let (producer, _) = spsc(buffer_size, || 0);
    ///
    /// let result = producer.into_exact::<10>();
    /// assert!(matches!(result, Err(Producer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(ExactProducer { .. })));
    /// ```
    pub fn into_exact<const BATCH: u32>(self) -> Result<ExactProducer<E, W, LEAD, BATCH>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(BATCH, self.buffer.size(), sequence) {
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
    /// Wait until at least `size` number of events are available.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer take longer to become available.
    pub fn wait(&mut self, size: u32) -> AvailableWrite<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + size as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.size() as i64
        } else {
            batch_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= desired_seq);
        AvailableWrite {
            cursor: &mut self.cursor,
            buffer: &mut self.buffer,
            current: producer_seq,
            end: batch_end,
            move_cursor: |cursor, _, end, ordering| cursor.sequence.store(end, ordering),
        }
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
    /// stall, as larger portions of the buffer take longer to become available.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableWrite<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + size as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.size() as i64
        } else {
            batch_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= desired_seq);
        Ok(AvailableWrite {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: producer_seq,
            end: batch_end,
            move_cursor: |cursor, _, end, ordering| cursor.sequence.store(end, ordering),
        })
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
    /// Returns the current sequence value for this producer's cursor.
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer.size()
    }

    /// Converts this [`ExactProducer`] into a [`Producer`].
    pub fn into_producer(self) -> Producer<E, W, LEAD> {
        Producer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        }
    }

    /// Returns an [`ExactProducer`] if `BATCH` is valid.
    ///
    /// Otherwise, returns the calling producer.
    ///
    /// Valid `BATCH` values must meet the following conditions:
    /// 1) `BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `BATCH`.
    ///
    /// # Examples
    /// ```
    /// let buffer_size = 64;
    /// let (producer, _) = ansa::spsc(64, || 0);
    /// let exact = producer.into_exact::<16>().unwrap();
    ///
    /// let result = exact.into_exact::<10>();
    /// assert!(matches!(result, Err(ansa::ExactProducer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact::<8>();
    /// assert!(matches!(result, Ok(ansa::ExactProducer { .. })));
    /// ```
    pub fn into_exact<const NEW: u32>(self) -> Result<ExactProducer<E, W, LEAD, NEW>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(NEW, self.buffer.size(), sequence) {
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
    /// Wait until at least `BATCH` number of events are available.
    pub fn wait(&mut self) -> AvailableWriteExact<'_, E, BATCH> {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + BATCH as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.size() as i64
        } else {
            batch_end
        };
        self.wait_strategy.wait(desired_seq, &self.barrier);
        debug_assert!(self.barrier.sequence() >= desired_seq);
        AvailableWriteExact {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: producer_seq,
            move_cursor: |cursor, _, end, ordering| cursor.sequence.store(end, ordering),
        }
    }
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactProducer<E, W, LEAD, BATCH>
where
    W: TryWaitStrategy,
{
    /// Wait until at least `BATCH` number of events are available.
    ///
    /// Otherwise, return the wait strategy error.
    pub fn try_wait(&mut self) -> Result<AvailableWriteExact<'_, E, BATCH>, W::Error> {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = producer_seq + BATCH as i64;
        let desired_seq = if LEAD {
            batch_end - self.buffer.size() as i64
        } else {
            batch_end
        };
        self.wait_strategy.try_wait(desired_seq, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= desired_seq);
        Ok(AvailableWriteExact {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current: producer_seq,
            move_cursor: |cursor, _, end, ordering| cursor.sequence.store(end, ordering),
        })
    }
}

/// A handle with immutable access to events on the ring buffer.
///
/// Can access events concurrently to handles with immutable access.
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

    /// Returns an [`ExactConsumer`] if `BATCH` is valid.
    ///
    /// Otherwise, returns the calling consumer.
    ///
    /// Valid `BATCH` values must meet the following conditions:
    /// 1) `BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `BATCH`.
    ///
    /// # Examples
    /// ```
    /// use ansa::{spsc, Consumer, ExactConsumer};
    ///
    /// let buffer_size = 64;
    /// let (_, consumer) = spsc(buffer_size, || 0);
    ///
    /// let result = consumer.into_exact::<10>();
    /// assert!(matches!(result, Err(Consumer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(ExactConsumer { .. })));
    /// ```
    pub fn into_exact<const BATCH: u32>(self) -> Result<ExactConsumer<E, W, BATCH>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(BATCH, self.buffer.size(), sequence) {
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
    /// Wait until at least `size` number of events are available.
    ///
    /// `size` must be less than the size of the buffer. Large `size`s may also cause handles to
    /// stall, as larger portions of the buffer take longer to become available.
    pub fn wait(&mut self, size: u32) -> AvailableRead<'_, E> {
        debug_assert!(size as usize <= self.buffer.size());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + size as i64;
        self.wait_strategy.wait(batch_end, &self.barrier);
        debug_assert!(self.barrier.sequence() >= batch_end);
        AvailableRead {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current_seq: consumer_seq,
            end_seq: batch_end,
        }
    }

    /// Wait until any number of events are available.
    pub fn wait_any(&mut self) -> AvailableRead<'_, E> {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.wait(consumer_seq + 1, &self.barrier);
        debug_assert!(barrier_seq > consumer_seq);
        AvailableRead {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current_seq: consumer_seq,
            end_seq: barrier_seq,
        }
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
    /// stall, as larger portions of the buffer take longer to become available.
    pub fn try_wait(&mut self, size: u32) -> Result<AvailableRead<'_, E>, W::Error> {
        debug_assert!(size as usize <= self.buffer.size());
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + size as i64;
        self.wait_strategy.try_wait(batch_end, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= batch_end);
        Ok(AvailableRead {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current_seq: consumer_seq,
            end_seq: batch_end,
        })
    }

    /// Wait until any number of events are available.
    ///
    /// Otherwise, returns the wait strategy error.
    pub fn try_wait_any(&mut self) -> Result<AvailableRead<'_, E>, W::Error> {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = self.wait_strategy.try_wait(consumer_seq + 1, &self.barrier)?;
        debug_assert!(barrier_seq > consumer_seq);
        Ok(AvailableRead {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current_seq: consumer_seq,
            end_seq: barrier_seq,
        })
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
    /// Returns the current sequence value for this consumer's cursor.
    pub fn sequence(&self) -> i64 {
        self.cursor.sequence.load(Ordering::Relaxed)
    }

    /// Returns the size of the ring buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer.size()
    }

    /// Converts this [`ExactConsumer`] into a [`Consumer`].
    pub fn into_consumer(self) -> Consumer<E, W> {
        Consumer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        }
    }

    /// Returns a new [`ExactConsumer`] if `NEW_BATCH` is valid.
    ///
    /// Otherwise, returns the calling consumer.
    ///
    /// Valid `NEW_BATCH` values must meet the following conditions:
    /// 1) `NEW_BATCH` must not be zero.
    /// 2) Buffer size must be divisible by `NEW_BATCH`.
    /// 3) This handle's sequence + 1 must be divisible by `NEW_BATCH`.
    ///
    /// # Examples
    /// ```
    /// let buffer_size = 64;
    /// let (_, consumer) = ansa::spsc(buffer_size, || 0);
    /// let exact = consumer.into_exact::<8>().unwrap();
    ///
    /// let result = exact.into_exact::<10>();
    /// assert!(matches!(result, Err(ansa::ExactConsumer { .. })));
    ///
    /// let result = result.unwrap_err().into_exact::<16>();
    /// assert!(matches!(result, Ok(ansa::ExactConsumer { .. })));
    /// ```
    pub fn into_exact<const NEW_BATCH: u32>(self) -> Result<ExactConsumer<E, W, NEW_BATCH>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if invalid_batch_size(NEW_BATCH, self.buffer.size(), sequence) {
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
    /// Wait until at least `BATCH` number of events are available.
    pub fn wait(&mut self) -> AvailableReadExact<'_, E, BATCH> {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + BATCH as i64;
        self.wait_strategy.wait(batch_end, &self.barrier);
        debug_assert!(self.barrier.sequence() >= batch_end);
        AvailableReadExact {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current_seq: consumer_seq,
        }
    }
}

impl<E, W, const BATCH: u32> ExactConsumer<E, W, BATCH>
where
    W: TryWaitStrategy,
{
    /// Wait until at least `BATCH` number of events are available.
    ///
    /// Otherwise, returns the wait strategy error.
    pub fn try_wait(&mut self) -> Result<AvailableReadExact<'_, E, BATCH>, W::Error> {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let batch_end = consumer_seq + BATCH as i64;
        self.wait_strategy.try_wait(batch_end, &self.barrier)?;
        debug_assert!(self.barrier.sequence() >= batch_end);
        Ok(AvailableReadExact {
            cursor: &mut self.cursor,
            buffer: &self.buffer,
            current_seq: consumer_seq,
        })
    }
}

#[inline]
fn invalid_batch_size(batch: u32, buffer_size: usize, sequence: i64) -> bool {
    batch == 0 || buffer_size % batch as usize != 0 || sequence % batch as i64 != 0
}

/// Represents a range of available sequences which may be written to.
pub struct AvailableWrite<'a, E> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current: i64,
    end: i64,
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
    pub fn write<F>(self, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        for seq in self.current + 1..=self.end {
            // SAFETY:
            // 1) Only one producer is accessing this sequence, hence only mutable ref will exist.
            // 2) Acquire-Release barrier ensures no other accesses to this sequence.
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == self.end);
        }
        (self.move_cursor)(self.cursor, self.current, self.end, Ordering::Release);
    }

    fn try_write_inner<F, Err, const COMMIT: bool>(self, mut write: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        for seq in self.current + 1..=self.end {
            // SAFETY:
            // 1) Only one producer is accessing this sequence, hence only mutable ref will exist.
            // 2) Acquire-Release barrier ensures no other accesses to this sequence.
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            if let err @ Err(_) = write(event, seq, seq == self.end) {
                if COMMIT {
                    (self.move_cursor)(self.cursor, self.current, self.end, Ordering::Release);
                }
                return err;
            }
        }
        (self.move_cursor)(self.cursor, self.current, self.end, Ordering::Release);
        Ok(())
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
    pub fn try_write<F, Err>(self, write: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        self.try_write_inner::<_, _, false>(write)
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
    pub fn try_write_commit<F, Err>(self, write: F) -> Result<(), Err>
    where
        F: FnMut(&mut E, i64, bool) -> Result<(), Err>,
    {
        self.try_write_inner::<_, _, true>(write)
    }
}

macro_rules! aliasing_model_validity {
    () => {
        "# Aliasing Model Validity\n\n\
        When checked with [miri](https://github.com/rust-lang/miri), this function is _valid_ \
        under the Tree-Borrows aliasing model, but _invalid_ under Stacked-Borrows.\n\n\
        If rust chooses to formally accept an aliasing model which declares this function invalid, \
        then its signature will be kept as-is, but its implementation will be changed to satisfy \
        the aliasing model. This may pessimize the function, but its API and semantics are \
        guaranteed to remain unchanged."
    };
}

/// Represents a range of available sequences which may be written to.
pub struct AvailableWriteExact<'a, E, const BATCH: u32> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current: i64,
    move_cursor: fn(&Arc<Cursor>, i64, i64, Ordering),
}

impl<E, const BATCH: u32> AvailableWriteExact<'_, E, BATCH> {
    /// Write an exact batch of events to the buffer.
    ///
    /// The parameters of `write` are:
    ///
    /// - `event: &mut E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    #[doc = aliasing_model_validity!()]
    pub fn write<F>(self, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        fence(Ordering::Acquire);
        let mut seq = self.current + 1;
        unsafe {
            // SAFETY:
            // 1) Only one producer is accessing this sequence, hence only mutable ref will exist.
            // 2) Acquire-Release barrier ensures no other accesses to this sequence.
            let mut pointer = self.buffer.get(seq);
            for _ in 0..BATCH - 1 {
                write(&mut *pointer, seq, false);
                // SAFETY: checks on BATCH guarantee this pointer to be inbounds of the buffer.
                pointer = pointer.add(1);
                seq += 1;
            }
            write(&mut *pointer, seq, true);
        }
        (self.move_cursor)(self.cursor, self.current, seq, Ordering::Release);
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
    #[doc = aliasing_model_validity!()]
    pub fn try_write<F, Err>(self, mut write: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        let mut seq = self.current + 1;
        unsafe {
            // SAFETY:
            // 1) Only one producer is accessing this sequence, hence only mutable ref will exist.
            // 2) Acquire-Release barrier ensures no other accesses to this sequence.
            let mut pointer = self.buffer.get(seq);
            for _ in 0..BATCH - 1 {
                write(&mut *pointer, seq, false)?;
                // SAFETY: checks on BATCH guarantee this pointer to be inbounds of the buffer.
                pointer = pointer.add(1);
                seq += 1;
            }
            write(&mut *pointer, seq, true)?;
        }
        (self.move_cursor)(self.cursor, self.current, seq, Ordering::Release);
        Ok(())
    }
}

/// Represents a range of available sequences which may be read from.
pub struct AvailableRead<'a, E> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current_seq: i64,
    end_seq: i64,
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
        for seq in self.current_seq + 1..=self.end_seq {
            // SAFETY:
            // 1) No mutable refs created here.
            // 2) Acquire-Release barrier ensures no attempted mutable accesses to this sequence.
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == self.end_seq);
        }
        self.cursor.sequence.store(self.end_seq, Ordering::Release);
    }

    fn try_read_inner<F, Err, const COMMIT: bool>(self, mut read: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        for seq in self.current_seq + 1..=self.end_seq {
            // SAFETY:
            // 1) No mutable refs created here.
            // 2) Acquire-Release barrier ensures no attempted mutable accesses to this sequence.
            let event: &E = unsafe { &*self.buffer.get(seq) };
            if let err @ Err(_) = read(event, seq, seq == self.end_seq) {
                if COMMIT {
                    self.cursor.sequence.store(seq - 1, Ordering::Release);
                }
                return err;
            }
        }
        self.cursor.sequence.store(self.end_seq, Ordering::Release);
        Ok(())
    }

    /// Read a batch of events from the buffer if successful.
    ///
    /// Otherwise, leave cursor sequence unchanged and return the error. Effectively returns to
    /// batch start.
    ///
    /// The parameters of `read` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    pub fn try_read<F, Err>(self, read: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        self.try_read_inner::<_, _, false>(read)
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
    pub fn try_read_commit<F, Err>(self, read: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        self.try_read_inner::<_, _, true>(read)
    }
}

/// Represents a range of available sequences which may be read from.
pub struct AvailableReadExact<'a, E, const BATCH: u32> {
    cursor: &'a mut Arc<Cursor>, // mutable ref ensures handle cannot run another overlapping wait
    buffer: &'a Arc<RingBuffer<E>>,
    current_seq: i64,
}

impl<E, const BATCH: u32> AvailableReadExact<'_, E, BATCH> {
    /// Read an exact batch of events from the buffer.
    ///
    /// The parameters of `read` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    #[doc = aliasing_model_validity!()]
    pub fn read<F>(self, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        fence(Ordering::Acquire);
        let mut seq = self.current_seq + 1;
        unsafe {
            // SAFETY:
            // 1) No mutable refs created here.
            // 2) Acquire-Release barrier ensures no attempted mutable accesses to this sequence.
            let mut pointer = self.buffer.get(seq) as *const E;
            for _ in 0..BATCH - 1 {
                read(&*pointer, seq, false);
                // SAFETY: checks on BATCH guarantee this pointer to be inbounds of the buffer.
                pointer = pointer.add(1);
                seq += 1;
            }
            read(&*pointer, seq, true);
        }
        self.cursor.sequence.store(seq, Ordering::Release);
    }

    /// Read an exact batch of events from the buffer if successful.
    ///
    /// Otherwise, leave cursor sequence unchanged and return the error. Effectively returns to
    /// batch start.
    ///
    /// The parameters of `read` are:
    ///
    /// - `event: &E`, a reference to the buffer element being accessed.
    /// - `sequence: i64`, the position of this event in the sequence.
    /// - `batch_end: bool`, indicating whether this is the last event in the requested batch.
    #[doc = aliasing_model_validity!()]
    pub fn try_read<F, Err>(self, mut read: F) -> Result<(), Err>
    where
        F: FnMut(&E, i64, bool) -> Result<(), Err>,
    {
        fence(Ordering::Acquire);
        let mut seq = self.current_seq + 1;
        unsafe {
            // SAFETY:
            // 1) No mutable refs created here.
            // 2) Acquire-Release barrier ensures no attempted mutable accesses to this sequence.
            let mut pointer = self.buffer.get(seq) as *const E;
            for _ in 0..BATCH - 1 {
                read(&*pointer, seq, false)?;
                // SAFETY: checks on BATCH guarantee this pointer to be inbounds of the buffer.
                pointer = pointer.add(1);
                seq += 1;
            }
            read(&*pointer, seq, true)?;
        }
        self.cursor.sequence.store(seq, Ordering::Release);
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
        assert_eq!(size_of::<ExactConsumer<u8, WaitBusy, 8>>(), 32);
        assert_eq!(size_of::<Producer<u8, WaitBusy, true>>(), 32);
        assert_eq!(size_of::<ExactProducer<u8, WaitBusy, true, 8>>(), 32);
        assert_eq!(size_of::<MultiProducer<u8, WaitBusy, true>>(), 40);
        assert_eq!(size_of::<ExactMultiProducer<u8, WaitBusy, true, 8>>(), 40);
    }
}
