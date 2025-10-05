// The code in this file is adapted from the following (MIT licensed) work:
// https://github.com/nicholassm/disruptor-rs/blob/bd15292e34d2c4bb53cba5709e0cf23e9753ebb8/src/ringbuffer.rs
// authored by: Nicholas Schultz-Møller

use core::cell::UnsafeCell;

/// A circular buffer of elements allocated on the heap. The buffer cannot be resized once created,
/// and its size must be a non-zero power of 2.
///
/// `mask` contains `(size - 1)`, which is half of what's needed to calculate a modulus with a
/// power of 2 number.
#[derive(Debug)]
pub(crate) struct RingBuffer<E> {
    buf: Box<[UnsafeCell<E>]>,
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
    /// Create a [`RingBuffer`] from a factory function.
    ///
    /// `size` must be a non-zero power of two. Callers must uphold this constraint.
    pub(crate) fn from_factory(size: usize, mut factory: impl FnMut() -> E) -> Self {
        debug_assert!(size > 0 && size.is_power_of_two());
        let buf = (0..size).map(|_| UnsafeCell::new(factory())).collect();
        let mask = (size - 1) as i64;
        RingBuffer { buf, mask }
    }

    /// Create a [`RingBuffer`] from an existing buffer.
    ///
    /// `buffer.len()` must be a non-zero power of two. Callers must uphold this constraint.
    pub(crate) const fn from_buffer(buf: Box<[E]>) -> Self {
        let size = buf.len();
        debug_assert!(size > 0 && size.is_power_of_two());
        // SAFETY: UnsafeCell<E> has the same size and layout as E
        let buf: Box<[UnsafeCell<E>]> = unsafe { core::mem::transmute(buf) };
        let mask = (size - 1) as i64;
        RingBuffer { buf, mask }
    }

    /// # Safety
    ///
    /// `start` and `end` must be inbounds of the ring buffer.
    #[inline]
    unsafe fn iter(&self, start: usize, end: usize) -> core::slice::Iter<'_, UnsafeCell<E>> {
        // SAFETY: caller must ensure that both start and end are inbounds of the buffer
        let slice = unsafe { self.buf.get_unchecked(start..end) };
        slice.iter()
    }

    /// Applies a closure to `size` number of buffer elements, starting from the element at `seq`.
    ///
    /// `size` must be less than the size of the buffer.
    ///
    /// `seq` must be non-negative.
    ///
    /// `seq` does not need to be an inbounds index of the buffer, the calculation:
    /// `seq mod buffer_size` is used to find which index to return an element pointer from.
    ///
    /// All pointers provided to the closure will be properly aligned and initialised, so
    /// dereferencing them can always be done safely.
    ///
    /// # Safety
    ///
    /// Creating a reference from a provided pointer is UB if mutable aliasing occurs as a result.
    /// Callers must satisfy both the following two conditions to create a reference safely:
    ///
    /// 1) Only **one** mutable reference to an element of the buffer may exist at any time.
    /// 2) No immutable reference may exist while a mutable reference to the same element exists.
    ///
    /// It is not enough for conflicting mutable and immutable references to not be used, they must
    /// not exist, as it is Undefined Behaviour for mutable aliasing to merely occur. Note that any
    /// number of immutable references can exist simultaneously, so long as no mutable ref exists
    /// during the lifetimes of those immutable refs.
    ///
    /// It is permitted for multiple mutable pointers to exist simultaneously, as the aliasing
    /// rules do not apply to pointers.
    #[inline]
    pub(crate) unsafe fn apply<F>(&self, seq: i64, size: usize, func: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        debug_assert!(seq >= 0);
        debug_assert!(size < self.size());
        let index = (seq & self.mask) as usize;
        // SAFETY: the first arg to self.iter will always be inbounds, while the second arg is
        // guaranteed inbounds by the caller ensuring `size < self.size()`.
        unsafe {
            let end = seq + size as i64 - 1;
            if index + size > self.size() {
                self.apply_wrapping(seq, size, index, end, func)
            } else {
                self.apply_direct(seq, size, index, end, func)
            }
        }
    }

    #[inline]
    unsafe fn apply_direct<F>(&self, seq: i64, size: usize, index: usize, end: i64, mut func: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        self.iter(index, index + size)
            .zip(seq..)
            .for_each(|(elem, s)| func(elem.get(), s, s == end))
    }

    #[cold]
    #[inline]
    unsafe fn apply_wrapping<F>(&self, seq: i64, size: usize, index: usize, end: i64, mut func: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        let diff = self.size() - index;
        self.iter(index, self.size())
            .zip(seq..)
            .chain(self.iter(0, size - diff).zip(seq + diff as i64..))
            .for_each(|(elem, s)| func(elem.get(), s, s == end))
    }

    #[cold]
    #[inline]
    pub(crate) unsafe fn apply_small<F>(&self, seq: i64, size: usize, mut func: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        debug_assert!(seq >= 0);
        debug_assert!(size < self.size());
        let end = seq + size as i64 - 1;
        (seq..=end).for_each(|s| {
            let index = (s & self.mask) as usize;
            // SAFETY: index will always be inbounds after masking
            let ptr = unsafe { self.buf.get_unchecked(index).get() };
            func(ptr, s, s == end)
        })
    }

    #[cold]
    #[inline]
    pub(crate) unsafe fn apply_one<F>(&self, seq: i64, mut func: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        debug_assert!(seq >= 0);
        let index = (seq & self.mask) as usize;
        // SAFETY: index will always be inbounds after masking
        let ptr = unsafe { self.buf.get_unchecked(index).get() };
        func(ptr, seq, true)
    }

    /// Tries to apply a closure to `size` number of buffer elements, starting from the element at
    /// `seq`.
    ///
    /// See [`RingBuffer::apply`] for more, and for **Safety** requirements.
    #[inline]
    pub(crate) unsafe fn try_apply<F, Err>(&self, seq: i64, size: usize, func: F) -> Result<(), Err>
    where
        F: FnMut(*mut E, i64, bool) -> Result<(), Err>,
    {
        let mut func = func;
        debug_assert!(seq >= 0);
        debug_assert!(size < self.size());
        let index = (seq & self.mask) as usize;
        // SAFETY: the first arg to self.iter will always be inbounds, while the second arg is
        // guaranteed inbounds by caller ensuring `size < self.size()`.
        unsafe {
            let end = seq + size as i64 - 1;
            if index + size > self.size() {
                // the requested batch wraps the buffer
                let diff = self.size() - index;
                self.iter(index, self.size())
                    .zip(seq..)
                    .try_for_each(|(elem, s)| func(elem.get(), s, false))?;

                self.iter(0, size - diff)
                    .zip(seq + diff as i64..)
                    .try_for_each(|(elem, s)| func(elem.get(), s, s == end))
            } else {
                self.iter(index, index + size)
                    .zip(seq..)
                    .try_for_each(|(elem, s)| func(elem.get(), s, s == end))
            }
        }
    }

    /// Returns the number of allocated buffer elements
    #[inline]
    pub(crate) const fn size(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wraps_batch_end_count() {
        let buffer = RingBuffer::from_factory(8, || 0u8);
        let mut end_count = 0;
        // with a batch the wraps the buffer, make sure end == true only once
        unsafe {
            buffer.apply(6, 4, |_, _, end| {
                if end {
                    end_count += 1;
                }
            })
        };
        assert_eq!(end_count, 1);
    }
}
