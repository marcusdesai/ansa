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
        debug_assert!(size > 0 && (size & (size - 1)) == 0);
        let buf = (0..size).map(|_| UnsafeCell::new(factory())).collect();
        let mask = size as i64 - 1;
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
        let mask = size as i64 - 1;
        RingBuffer { buf, mask }
    }

    /// Returns a mutable pointer to a single element of the ring buffer.
    ///
    /// `sequence` must be non-negative.
    ///
    /// `sequence` does not need to be an inbounds index of the buffer, the calculation:
    /// `sequence mod buffer_size` is used to find which index to return an element pointer from.
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
    /// during the lifetimes of those immutable refs.
    ///
    /// Also note that the aliasing rules do not apply to pointers, so it is permitted for multiple
    /// mutable pointers to exist simultaneously.
    #[inline]
    unsafe fn get(&self, sequence: i64) -> *mut E {
        debug_assert!(sequence >= 0);
        // sequence may be greater than buffer size, so mod to bring it inbounds. Size is a power
        // of 2, so the calculation is `sequence & (size - 1)`, which is `sequence & self.mask`
        let index = sequence & self.mask;
        // SAFETY: index will be inbounds after mod
        let cell = unsafe { self.buf.get_unchecked(index as usize) };
        cell.get()
    }

    #[inline]
    fn iter(&self, s: usize, e: usize, seq: i64) -> impl Iterator<Item = (*mut E, i64, bool)> + '_ {
        let slice = unsafe { self.buf.get_unchecked(s..e) };
        slice
            .iter()
            .enumerate()
            .map(move |(i, elem)| (elem.get(), seq + i as i64, s + i == e - 1))
    }

    /// Applies a closure to `size` number of buffer elements, starting from the element at `seq`.
    ///
    /// SAFETY: see [`RingBuffer::get`].
    #[inline]
    pub(crate) unsafe fn apply<F>(&self, seq: i64, size: i64, mut func: F)
    where
        F: FnMut((*mut E, i64, bool)),
    {
        if size == 0 {
            return;
        }
        let size = size as usize;
        debug_assert!(seq >= 0);
        debug_assert!(size < self.buf.len());
        let index = (seq & self.mask) as usize;
        if index + size > self.buf.len() {
            //the requested batch wraps the buffer
            let diff = self.buf.len() - index;
            self.iter(index, self.buf.len(), seq).for_each(&mut func);
            self.iter(0, size - diff, seq + diff as i64).for_each(&mut func);
        } else {
            self.iter(index, index + size, seq).for_each(func)
        }
    }

    /// Applies a closure to `size` number of buffer elements, starting from the element at `seq`.
    ///
    /// SAFETY: see [`RingBuffer::get`].
    #[inline]
    pub(crate) unsafe fn try_apply<F, Err>(&self, seq: i64, size: i64, func: F) -> Result<(), Err>
    where
        F: FnMut(*mut E, i64, bool) -> Result<(), Err>,
    {
        if size == 0 {
            return Ok(());
        }
        let mut seq = seq;
        let mut func = func;
        debug_assert!(seq >= 0);
        debug_assert!(size < self.size());
        let index = seq & self.mask;
        // whether the requested batch wraps the buffer
        let wraps = index + size > self.size();
        // SAFETY: index will always be inbounds after BitAnd with mask
        let mut ptr = unsafe { self.buf.get_unchecked(index as usize).get() };

        let end = seq + size - 1;
        loop {
            if seq == end {
                func(ptr, seq, true)?;
                break Ok(());
            }
            func(ptr, seq, false)?;
            seq += 1;
            if !wraps && cfg!(feature = "tree-borrows") {
                // Only passes miri with MIRIFLAGS=-Zmiri-tree-borrows
                ptr = unsafe { ptr.add(1) };
            } else {
                ptr = unsafe { self.get(seq) };
            }
        }
    }

    /// Returns the number of allocated buffer elements
    #[inline]
    pub(crate) fn size(&self) -> i64 {
        // SAFETY: conversion fine as allocations cannot be larger than i64::MAX
        unsafe { self.buf.len().try_into().unwrap_unchecked() }
    }
}
