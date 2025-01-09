use crate::ringbuffer::RingBuffer;
use crate::wait::WaitStrategy;
use std::sync::atomic::{fence, AtomicI64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiProducer<E, W> {
    pub(crate) cursor: Arc<Cursor>, // shared between all multi producers and as a barrier for consumers
    pub(crate) barrier: Barrier,    // This must be made up of the last consumer cursors.
    pub(crate) buffer: Arc<RingBuffer<E>>, // shared between all consumers and producers
    pub(crate) claim: Arc<Cursor>,  // shared between all multi producers
    pub(crate) barrier_seq: i64,
    pub(crate) wait_strategy: W,
}

impl<E, W> MultiProducer<E, W>
where
    W: WaitStrategy,
{
    pub fn batch_write<F>(&mut self, size: u32, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(
            (size as usize) <= self.buffer.len(),
            "Batch size cannot be greater than buffer size"
        );
        let size = size as i64;
        // Claim a sequence. The 'claim' is used to coordinate producers.
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
        // Wait for the last consumer barrier to move past the end of the claim. This calculation
        // utilises the constraint that `claim_end` >= `self.barrier_seq`. Note that, because
        // `claim_end` does not represent the position of the producer cursor, it is allowed to
        // hold a value greater than the position of the last consumer cursor on the ring buffer.
        while producer_barrier_delta(self.buffer.len(), claim_end, self.barrier_seq) < 0 {
            self.wait_strategy.wait();
            self.barrier_seq = self.barrier.sequence();
        }
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
        // Now wait for producer cursor to catch up to start of claimed sequence. This ensures that
        // writes later in the sequence are not made visible to consumers until all earlier writes
        // are completed. Without this check, we might accidentally make unfinished writes visible
        // which could cause overlapping immutable and mutable refs to be created.
        let mut cursor_seq = self.cursor.sequence.load(Ordering::Relaxed);
        while cursor_seq != current_claim {
            self.wait_strategy.wait();
            cursor_seq = self.cursor.sequence.load(Ordering::Relaxed);
        }
        // Ensure synchronisation occurs by creating an Acquire-Release barrier. `store` doesn't
        // accept `AcqRel`, so use a fence to make the pair.
        fence(Ordering::Acquire);
        // Finally, advance producer cursor to publish the writes upto the end of the claimed
        // sequence.
        self.cursor.sequence.store(claim_end, Ordering::Release);
        self.wait_strategy.finalise()
    }
}

impl<E, W> Clone for MultiProducer<E, W>
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

impl<E, W> Drop for MultiProducer<E, W> {
    fn drop(&mut self) {
        // We need to break the Arc cycle of barriers. Simply get rid of all the `Arc`s this
        // producer holds to guarantee this.
        self.barrier = Barrier::Many(Box::new([]))
    }
}

#[derive(Debug)]
pub struct SingleProducer<E, W> {
    pub(crate) cursor: Arc<Cursor>, // shared by this producer and as barrier for consumers
    pub(crate) barrier: Barrier,    // This must be the last consumer cursors.
    pub(crate) buffer: Arc<RingBuffer<E>>, // shared between all consumers and producers
    pub(crate) free_slots: i64,
    pub(crate) wait_strategy: W,
}

impl<E, W> SingleProducer<E, W>
where
    W: WaitStrategy,
{
    pub fn batch_write<F>(&mut self, size: u32, mut write: F)
    where
        F: FnMut(&mut E, i64, bool),
    {
        assert!(
            (size as usize) <= self.buffer.len(),
            "Batch size cannot be greater than buffer size"
        );
        let size = size as i64;
        let producer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        // Wait until there are free slots to write events to.
        while self.free_slots < size {
            self.wait_strategy.wait();
            let barrier_seq = self.barrier.sequence();
            assert!(
                barrier_seq <= producer_seq,
                "Producer cursor sequence must never be behind of its barrier sequence.\nFound \
                producer seq: {producer_seq}, and barrier seq: {barrier_seq}"
            );
            self.free_slots = producer_barrier_delta(self.buffer.len(), producer_seq, barrier_seq);
        }
        // Ensure synchronisation occurs by creating an Acquire-Release barrier. `store` doesn't
        // accept `AcqRel`, so use a fence to make the pair.
        fence(Ordering::Acquire);
        // Begin writing events to the buffer.
        let batch_end = producer_seq + size;
        for seq in producer_seq + 1..=batch_end {
            // SAFETY:
            // 1) We know that there is only one producer, so multiple mutable refs cannot exist.
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
}

impl<E, W> Drop for SingleProducer<E, W> {
    fn drop(&mut self) {
        // We need to break the Arc cycle of barriers. Simply get rid of all the `Arc`s this
        // producer holds to guarantee this.
        self.barrier = Barrier::Many(Box::new([]))
    }
}

#[derive(Debug)]
pub struct Consumer<E, W> {
    pub(crate) cursor: Arc<Cursor>, // shared between this consumer and as a barrier for consumers or producers
    pub(crate) barrier: Barrier,
    pub(crate) buffer: Arc<RingBuffer<E>>, // shared between all consumers and producers
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
            "Batch size cannot be greater than buffer size"
        );
        let size = size as i64;
        let consumer_seq = self.cursor.sequence.load(Ordering::Relaxed);
        let mut barrier_seq = self.barrier.sequence();
        // Wait until there are events to read.
        while barrier_seq - consumer_seq < size {
            assert!(
                consumer_seq <= barrier_seq,
                "Consumer cursor sequence must never be ahead of its barrier sequence.\nFound \
                consumer seq: {consumer_seq}, and barrier seq: {barrier_seq}"
            );
            self.wait_strategy.wait();
            barrier_seq = self.barrier.sequence();
        }
        // Ensure synchronisation occurs by creating an Acquire-Release barrier. `store` doesn't
        // accept `AcqRel`, so use a fence to make the pair.
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
                "Consumer cursor sequence must never be ahead of its barrier sequence.\nFound \
                consumer seq: {consumer_seq}, and barrier seq: {barrier_seq}"
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
    pub(crate) fn new() -> Self {
        Cursor {
            #[cfg(not(feature = "cache-padded"))]
            sequence: AtomicI64::new(0),
            #[cfg(feature = "cache-padded")]
            sequence: crossbeam_utils::CachePadded::new(AtomicI64::new(0)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Barrier {
    One(Arc<Cursor>),
    Many(Box<[Arc<Cursor>]>),
}

impl Barrier {
    fn sequence(&self) -> i64 {
        match self {
            Barrier::One(cursor) => cursor.sequence.load(Ordering::Relaxed),
            Barrier::Many(cursors) => cursors.iter().fold(i64::MAX, |seq, cursor| {
                seq.min(cursor.sequence.load(Ordering::Relaxed))
            }),
        }
    }
}

/// Calculate the distance on the ring buffer from a producer provided sequence to the barrier of
/// the last consumers. Positive return values represent free slots available for writes on the
/// buffer. Negative values represent the producer sequence being ahead of the barrier by the
/// returned amount.
///
/// This calculation utilises the constraint that 0 <= barrier sequence <= producer sequence.
/// Callers must ensure that this constraint always holds.
#[inline]
fn producer_barrier_delta(buffer_size: usize, producer_seq: i64, barrier_seq: i64) -> i64 {
    assert!(
        0 <= barrier_seq && barrier_seq <= producer_seq,
        "Requires that 0 <= barrier sequence <= producer sequence.\n\
        Found producer seq: {producer_seq}, and barrier seq: {barrier_seq}",
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
