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
    mask: usize,
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
        debug_assert!(size > 0 && (size & (size - 1)) == 0);
        let buf = (0..size).map(|_| UnsafeCell::new(factory())).collect();
        let mask = size - 1;
        RingBuffer { buf, mask }
    }

    /// Create a [`RingBuffer`] from an existing buffer.
    ///
    /// `size` must be a non-zero power of two. Callers must uphold this constraint.
    pub(crate) fn from_buffer(buf: Box<[E]>) -> Self {
        let size = buf.len();
        debug_assert!(size > 0 && (size & (size - 1)) == 0);
        // SAFETY: UnsafeCell<E> has the same size and layout as E
        let buf: Box<[UnsafeCell<E>]> = unsafe { std::mem::transmute(buf) };
        let mask = size - 1;
        RingBuffer { buf, mask }
    }

    /// # Safety
    ///
    /// `start` and `end` must be inbounds of the ring buffer.
    #[inline]
    unsafe fn iter(&self, start: usize, end: usize) -> std::slice::Iter<'_, UnsafeCell<E>> {
        // SAFETY: caller must ensure that both start and end are inbounds of the buffer
        let slice = unsafe { self.buf.get_unchecked(start..end) };
        slice.iter()
    }

    /// Applies a closure to `size` number of buffer elements, starting from the element at `seq`.
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
    /// Also note that the aliasing rules do not apply to pointers, so it is permitted for multiple
    /// mutable pointers to exist simultaneously.
    #[inline]
    pub(crate) unsafe fn apply<F>(&self, seq: i64, size: usize, mut func: F)
    where
        F: FnMut(*mut E, i64, bool),
    {
        if size == 0 {
            return;
        }
        debug_assert!(seq >= 0);
        debug_assert!(size < self.size());
        // if seq is cast to usize before masking, it may be incorrectly truncated.
        let index = (seq & self.mask as i64) as usize;
        // SAFETY: the first arg to self.iter will always be inbounds, while the second arg is
        // guaranteed inbounds by caller ensuring `size < self.size()`.
        unsafe {
            let end = seq + size as i64 - 1;
            if index + size > self.size() {
                // the requested batch wraps the buffer
                let diff = self.size() - index;
                let iter = self.iter(index, self.size()).chain(self.iter(0, size - diff));
                iter.zip(seq..).for_each(|(elem, s)| func(elem.get(), s, s == end))
            } else {
                let iter = self.iter(index, index + size);
                iter.zip(seq..).for_each(|(elem, s)| func(elem.get(), s, s == end))
            }
        }
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
        if size == 0 {
            return Ok(());
        }
        let mut func = func;
        debug_assert!(seq >= 0);
        debug_assert!(size < self.size());
        // if seq is cast to usize before masking, it may be incorrectly truncated.
        let index = (seq & self.mask as i64) as usize;
        // SAFETY: the first arg to self.iter will always be inbounds, while the second arg is
        // guaranteed inbounds by caller ensuring `size < self.size()`.
        unsafe {
            let end = seq + size as i64 - 1;
            if index + size > self.size() {
                // the requested batch wraps the buffer
                let diff = self.size() - index;
                let iter = self.iter(index, self.size()).chain(self.iter(0, size - diff));
                iter.zip(seq..).try_for_each(|(elem, s)| func(elem.get(), s, s == end))
            } else {
                let iter = self.iter(index, index + size);
                iter.zip(seq..).try_for_each(|(elem, s)| func(elem.get(), s, s == end))
            }
        }
    }

    /// Returns the number of allocated buffer elements
    #[inline]
    pub(crate) fn size(&self) -> usize {
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
