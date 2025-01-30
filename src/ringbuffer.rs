// The code in this file is adapted from the following (MIT licensed) work:
// https://github.com/nicholassm/disruptor-rs/blob/bd15292e34d2c4bb53cba5709e0cf23e9753ebb8/src/ringbuffer.rs
// authored by: Nicholas Schultz-Møller

use std::cell::UnsafeCell;

/// A circular buffer of elements allocated on the heap. The buffer cannot be resized once created,
/// and its size must be a power of 2.
///
/// `mask` contains `(size - 1)`, which is half of what's needed to calculate a modulus with a
/// power of 2 number.
#[derive(Debug)]
pub(crate) struct RingBuffer<E> {
    slots: Box<[UnsafeCell<E>]>,
    mask: i64,
}

// SAFETY: `RingBuffer` provides an API that does not, by itself, guarantee safe references to a
// single `RingBuffer` across multiple threads. In a single thread, accesses to `RingBuffer`
// elements, using `RingBuffer::get`, must ensure that no mutable aliasing occurs. With multiple
// threads, accesses to elements must also ensure no mutable aliasing occurs across threads.
//
// Note: `E` must be `Sync` because it must be possible to refer to `E` across multiple threads.
unsafe impl<E> Sync for RingBuffer<E> where E: Sync {}

impl<E> RingBuffer<E> {
    /// Create a [`RingBuffer`]. Uses a boxed slice for storage, ensuring data is held contiguously
    /// in memory.
    ///
    /// `size` must be a non-zero power of two. Error flagging for invalid sizes is handled at a
    /// higher level in the API, so callers must uphold this constraint.
    pub(crate) fn new<F>(size: usize, mut factory: F) -> Self
    where
        F: FnMut() -> E,
    {
        assert!(
            size > 0 && (size & (size - 1)) == 0,
            "size must be non-zero power of 2; found: {size}"
        );
        let slots: Box<[UnsafeCell<E>]> = (0..size).map(|_| UnsafeCell::new(factory())).collect();
        RingBuffer {
            slots,
            mask: size as i64 - 1,
        }
    }

    /// Returns a mutable pointer to a single element of the ring buffer.
    ///
    /// `sequence` does not need to be an inbounds index of the buffer, any non-negative `i64` is
    /// accepted. The calculation: `sequence mod buffer_size` is used to find which index to return
    /// an element pointer from.
    ///
    /// The returned pointer is guaranteed to be properly aligned and initialised, so dereferencing
    /// can always be done safely.
    ///
    /// # Safety
    /// Creating a reference from the pointer returned from this function is UB if mutable
    /// aliasing occurs as a result. Callers must satisfy both the following two conditions to
    /// create a reference safely:
    /// 1) Only **one** mutable reference to an element of the buffer may exist at any time.
    /// 2) No immutable reference may exist while a mutable reference to the same element exists.
    ///
    /// It is not enough for conflicting mutable and immutable references to not be used, they must
    /// not exist, as it is Undefined Behaviour for mutable aliasing to merely occur. Note that any
    /// number of immutable references can exist simultaneously, so long as no mutable ref exists
    /// during the lifetimes of those immutable refs. Also note that the aliasing rules do not
    /// apply to pointers, so it is permitted for multiple mutable pointers to exist simultaneously.
    #[inline]
    pub(crate) fn get(&self, sequence: i64) -> *mut E {
        assert!(sequence >= 0, "sequence must be >= 0; found: {sequence}");
        // sequence may be greater than buffer size, so mod to bring it inbounds. Since size is a
        // power of 2, the calculation is: `sequence & (size - 1)`, and we've already calculated
        // `(size - 1)`
        let index = sequence & self.mask;
        // SAFETY: index will be inbounds after mod
        let cell = unsafe { self.slots.get_unchecked(index as usize) };
        cell.get()
    }

    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.slots.len()
    }
}
