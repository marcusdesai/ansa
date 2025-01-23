use crate::ringbuffer::RingBuffer;
use crate::wait::WaitStrategy;
use std::sync::atomic::{fence, AtomicI64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiProducer<E, W, const LEAD: bool> {
    cursor: Arc<Cursor>,
    barrier: Barrier,
    buffer: Arc<RingBuffer<E>>,
    claim: Arc<Cursor>, // shared between all clones of this multi producer
    barrier_seq: i64,
    wait_strategy: W,
}

impl<E, W, const LEAD: bool> MultiProducer<E, W, LEAD>
where
    W: WaitStrategy,
{
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
        if LEAD {
            // Utilise the constraint that `claim_end` >= `barrier_seq`. Note that `claim_end` does
            // not represent the position of the producer cursor, so it is allowed to hold a value
            // ahead of the producer barrier.
            while producer_barrier_delta(self.buffer.len(), claim_end, self.barrier_seq) < 0 {
                self.wait_strategy.wait();
                self.barrier_seq = self.barrier.sequence();
            }
        } else {
            // If this is not the lead producer we need to wait until `claim_end` <= `barrier_seq`
            while claim_end > self.barrier_seq {
                self.wait_strategy.wait();
                self.barrier_seq = self.barrier.sequence();
            }
        }
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
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == claim_end);
        }
        // Wait for the cursor to catch up to start of claimed sequence. This ensures that writes
        // later in the sequence are not made visible until all earlier writes by this multi
        // producer or its clones are completed. Without this check, unfinished writes may become
        // prematurely visible, allowing overlapping immutable and mutable refs to be created.
        let mut cursor_seq = self.cursor.sequence.load(Ordering::Acquire);
        while cursor_seq != current_claim {
            self.wait_strategy.wait();
            cursor_seq = self.cursor.sequence.load(Ordering::Acquire);
        }
        // Finally, advance producer cursor to publish the writes upto the end of the claimed
        // sequence.
        self.cursor.sequence.store(claim_end, Ordering::Release);
        self.wait_strategy.finalise()
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
            free_slots: 0,
            wait_strategy: self.wait_strategy,
        })
    }
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
            barrier_seq: self.barrier_seq,
            wait_strategy: self.wait_strategy.clone(),
        }
    }
}

#[derive(Debug)]
pub struct Producer<E, W, const LEAD: bool> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) free_slots: i64,
    pub(crate) wait_strategy: W,
}

impl<E, W, const LEAD: bool> Producer<E, W, LEAD>
where
    W: WaitStrategy,
{
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
        while self.free_slots < size {
            self.wait_strategy.wait();
            let barrier_seq = self.barrier.sequence();
            if LEAD {
                let buf_len = self.buffer.len();
                self.free_slots = producer_barrier_delta(buf_len, producer_seq, barrier_seq);
            } else {
                self.free_slots = barrier_seq - producer_seq;
            }
        }
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
            let event: &mut E = unsafe { &mut *self.buffer.get(seq) };
            write(event, seq, seq == batch_end);
        }
        self.free_slots -= size;
        // Move cursor upto the end of the written batch.
        self.cursor.sequence.store(batch_end, Ordering::Release);
        self.wait_strategy.finalise()
    }

    pub fn into_multi(self) -> MultiProducer<E, W, LEAD> {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let barrier_seq = (producer_seq - self.buffer.len() as i64).max(0);
        MultiProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            claim: Arc::new(Cursor::new(producer_seq)),
            barrier_seq,
            wait_strategy: self.wait_strategy,
        }
    }

    pub fn into_exact<const BATCH: u32>(self) -> Result<ExactProducer<E, W, LEAD, BATCH>, Self> {
        let sequence = self.cursor.sequence.load(Ordering::Relaxed) + 1;
        if BATCH == 0 || self.buffer.len() % BATCH as usize != 0 || sequence % BATCH as i64 != 0 {
            return Err(self);
        }
        Ok(ExactProducer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            free_slots: self.free_slots,
            wait_strategy: self.wait_strategy,
        })
    }
}

#[derive(Debug)]
pub struct ExactProducer<E, W, const LEAD: bool, const BATCH: u32> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) free_slots: i64,
    pub(crate) wait_strategy: W,
}

impl<E, W, const LEAD: bool, const BATCH: u32> ExactProducer<E, W, LEAD, BATCH>
where
    W: WaitStrategy,
{
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
    pub fn write_exact<F>(&mut self, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        // Wait until there are free slots to write events to.
        while self.free_slots < BATCH as i64 {
            self.wait_strategy.wait();
            let barrier_seq = self.barrier.sequence();
            if LEAD {
                let buf_len = self.buffer.len();
                self.free_slots = producer_barrier_delta(buf_len, producer_seq, barrier_seq);
            } else {
                self.free_slots = barrier_seq - producer_seq;
            }
        }
        // Ensure synchronisation occurs by creating an Acquire-Release barrier for the entire
        // duration of the writes.
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) We know that there is only one producer accessing this section of the buffer, so
        //    multiple mutable refs cannot exist.
        // 2) The Acquire-Release memory barrier ensures this memory location will not be read
        //    while it is written to. This ensures that immutable refs will not be created for
        //    this element while the mutable ref exists.
        // 3) The pointer is guaranteed to be inbounds of the ring buffer allocation by the checks
        //    on BATCH size made when creating this struct.
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
        self.free_slots -= BATCH as i64;
        // Move cursor upto the end of the written batch.
        self.cursor.sequence.store(seq, Ordering::Release);
        self.wait_strategy.finalise()
    }

    pub fn into_producer(self) -> Producer<E, W, LEAD> {
        Producer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            free_slots: self.free_slots,
            wait_strategy: self.wait_strategy,
        }
    }
}

#[derive(Debug)]
pub struct Consumer<E, W> {
    pub(crate) cursor: Arc<Cursor>,
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>,
    pub(crate) wait_strategy: W,
}

impl<E, W> Consumer<E, W>
where
    W: WaitStrategy,
{
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
        let mut barrier_seq = self.barrier.sequence();
        // Wait until there are events to read.
        while barrier_seq - consumer_seq < size {
            assert!(
                consumer_seq <= barrier_seq,
                "consumer_seq ({consumer_seq}) > barrier_seq ({barrier_seq})"
            );
            self.wait_strategy.wait();
            barrier_seq = self.barrier.sequence();
        }
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
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == batch_end);
        }
        // Move cursor up to barrier sequence.
        self.cursor.sequence.store(batch_end, Ordering::Release);
        self.wait_strategy.finalise()
    }

    /// Specialised function which will always consume *all* available buffer elements when called.
    pub fn read<F>(&self, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let mut barrier_seq = self.barrier.sequence();
        while barrier_seq - consumer_seq == 0 {
            assert!(
                consumer_seq <= barrier_seq,
                "consumer_seq ({consumer_seq}) > barrier_seq ({barrier_seq})"
            );
            self.wait_strategy.wait();
            barrier_seq = self.barrier.sequence();
        }
        fence(Ordering::Acquire);
        for seq in consumer_seq + 1..=barrier_seq {
            // SAFETY:
            // 1) The mutable pointer to the event is immediately converted to an immutable ref,
            //    ensuring multiple mutable refs do not exist.
            // 2) The Acquire-Release synchronisation ensures that the consumer cursor does not
            //    visibly update its value until all the events are processed, which in turn ensures
            //    that producers will not write here while the consumer is reading. This ensures
            //    that no mutable ref to this element is created while this immutable ref exists.
            let event: &E = unsafe { &*self.buffer.get(seq) };
            read(event, seq, seq == barrier_seq);
        }
        self.cursor.sequence.store(barrier_seq, Ordering::Release);
        self.wait_strategy.finalise()
    }

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
        if BATCH == 0 || self.buffer.len() % BATCH as usize != 0 || sequence % BATCH as i64 != 0 {
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

#[derive(Debug)]
pub struct ExactConsumer<E, W, const BATCH: u32> {
    cursor: Arc<Cursor>,
    barrier: Barrier,
    buffer: Arc<RingBuffer<E>>,
    wait_strategy: W,
}

impl<E, W, const BATCH: u32> ExactConsumer<E, W, BATCH>
where
    W: WaitStrategy,
{
    pub fn read_exact<F>(&self, mut read: F)
    where
        F: FnMut(&E, i64, bool),
    {
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let mut barrier_seq = self.barrier.sequence();
        while barrier_seq - consumer_seq < BATCH as i64 {
            assert!(
                consumer_seq <= barrier_seq,
                "consumer_seq ({consumer_seq}) > barrier_seq ({barrier_seq})"
            );
            self.wait_strategy.wait();
            barrier_seq = self.barrier.sequence();
        }
        fence(Ordering::Acquire);
        // SAFETY:
        // 1) The mutable pointer to the event is immediately converted to an immutable pointer,
        //    ensuring multiple mutable refs cannot be created here.
        // 2) The Acquire-Release synchronisation ensures that the consumer cursor does not visibly
        //    update its value until all the events are processed, which in turn ensures that
        //    producers will not write here while the consumer is reading. This ensures that no
        //    mutable ref to this element is created while this immutable ref exists.
        // 3) The pointer is guaranteed to be inbounds of the ring buffer allocation by the checks
        //    on BATCH size made when creating this struct.
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
        self.wait_strategy.finalise()
    }

    pub fn into_consumer(self) -> Consumer<E, W> {
        Consumer {
            cursor: self.cursor,
            barrier: self.barrier,
            buffer: self.buffer,
            wait_strategy: self.wait_strategy,
        }
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

    /// Create a cursor at the start of the sequence. Since all reads and writes begin on the
    /// _next_ position in the sequence, we start at `-1` so that reads and writes start at `0`
    pub(crate) const fn start() -> Self {
        Cursor::new(-1)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Barrier {
    One(Arc<Cursor>),
    Many(Box<[Arc<Cursor>]>),
}

impl Barrier {
    #[inline]
    fn sequence(&self) -> i64 {
        match self {
            Barrier::One(cursor) => cursor.sequence.load(Ordering::Relaxed),
            Barrier::Many(cursors) => cursors.iter().fold(i64::MAX, |seq, cursor| {
                seq.min(cursor.sequence.load(Ordering::Relaxed))
            }),
        }
    }
}

impl Drop for Barrier {
    // We need to break the Arc cycle of barriers. Just get rid of all the Arcs to guarantee this.
    fn drop(&mut self) {
        match self {
            Barrier::One(one) => *one = Arc::new(Cursor::new(0)),
            Barrier::Many(many) => *many = Box::new([]),
        }
    }
}

/// Calculate the distance on the ring buffer from a producer provided sequence to its barrier.
/// Positive return values represent available distance to the barrier. Zero or negative values
/// represent no available write distance.
///
/// This calculation utilises the constraint that: `-1 <= barrier_seq <= producer_seq`, which is
/// only true for lead producers. Callers must ensure that this holds.
#[inline]
fn producer_barrier_delta(buffer_size: usize, producer_seq: i64, barrier_seq: i64) -> i64 {
    assert!(
        -1 <= barrier_seq && barrier_seq <= producer_seq,
        "Constraint error: 0 <= barrier_seq ({barrier_seq}) <= producer_seq ({producer_seq})"
    );
    buffer_size as i64 - (producer_seq - barrier_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn test_producer_barrier_delta() {
        let mut rng = thread_rng();
        // calculation should match buffer size - (producer sequence - barrier sequence)
        let prod_seq = rng.gen_range(100..i64::MAX / 2);
        assert_eq!(producer_barrier_delta(16, prod_seq, prod_seq - 20), -4);

        let same_seq = rng.gen_range(0..i64::MAX / 2);
        assert_eq!(producer_barrier_delta(16, same_seq, same_seq), 16);

        let prod_seq = rng.gen_range(100..i64::MAX / 2);
        assert_eq!(producer_barrier_delta(16, prod_seq, prod_seq - 12), 4);

        let prod_seq = rng.gen_range(100..i64::MAX / 2);
        assert_eq!(producer_barrier_delta(16, prod_seq, prod_seq - 16), 0);
    }
}
