//! # Ansa
//!
//! Multithreaded, lock-free queue implementation using the Disruptor pattern.
//!
//! todo
//!
//! to mention:
//! - no panics outside of the convenience functions
//! - sequence limit (i64::MAX)
//! - Basics of how the disruptor works
//! - traffic jam analogy
//! - features
//! - examples
//! - terminology (ring buffer, handle, etc...)
//!
//! # Disruptor
//!
//! This crate implements the Disruptor pattern as described in the original [whitepaper][disruptor].
//! In particular, section `4.3 Sequencing` provides a complete description of how accesses to the
//! buffer are synchronised. It is not an understatement to say that understanding just section 4.3
//! of the paper is enough to understand the Disruptor pattern as a whole.
//!
//! [disruptor]: https://github.com/LMAX-Exchange/disruptor/blob/0a5adf2bf35ba508b11fa69fa592d28529e50679/src/docs/files/Disruptor-1.0.pdf
//!
//! A disruptor is made up of three components:
//! - A ring buffer (aka circular buffer, aka buffer).
//! - A lead producer handle which follows the last of the trailing handles.
//! - Any number of consumer and producer handles which trail the lead, and which may be structured
//!   into a [directed acyclic graph][dag].
//!
//! [dag]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
//!
//! The buffer is pre-populated with events when a disruptor is created.
//!
//! As their names suggest, produces have mutable access to events on the buffer, while consumers
//! have only immutable access.
//!
//! Every handle is limited in what portion of the buffer it can access by the handles it follows.
//! A handle `h` *cannot* overtake the handles `B` (for barrier) that it follows. This constraint
//! is ensures buffer accesses do not invalidly overlap.
//!

mod builder;
mod handles;
mod ringbuffer;
pub mod wait;

pub use builder::*;
pub use handles::*;

use crate::wait::{WaitPhased, WaitSleep};

/// Construct a Single-Producer Single-Consumer disruptor.
///
/// `size` must be a non-zero power of two. `event_factory` is used to populate the buffer.
///
/// Uses a [`WaitPhased<WaitSleep>`](WaitPhased) strategy which busy-spins for 1 millisecond, then
/// spins and yields the thread for 1 millisecond, and finally spins and sleeps for 50 microseconds.
///
/// See: [`DisruptorBuilder`] for configurable disruptor construction.
///
/// # Panics
///
/// If `size` is zero or not a power of two.
///
/// # Examples
/// ```
/// let (producer, consumer) = ansa::spsc(64, || 0);
/// ```
pub fn spsc<E>(
    size: usize,
    event_factory: impl FnMut() -> E,
) -> (
    Producer<E, WaitPhased<WaitSleep>, true>,
    Consumer<E, WaitPhased<WaitSleep>>,
)
where
    E: Sync,
{
    assert!(
        size > 0 && (size & (size - 1)) == 0,
        "size ({size}) must be non-zero power of two"
    );
    let mut handles = DisruptorBuilder::new(size, event_factory)
        .add_handle(0, Handle::Consumer, Follows::LeadProducer)
        .build()
        .unwrap();
    let producer = handles.take_lead().unwrap();
    let consumer = handles.take_consumer(0).unwrap();
    (producer, consumer)
}

/// Construct a Multi-Producer Single-Consumer disruptor.
///
/// `size` must be a non-zero power of two. `event_factory` is used to populate the buffer.
///
/// Uses a [`WaitPhased<WaitSleep>`](WaitPhased) strategy which busy-spins for 1 millisecond, then
/// spins and yields the thread for 1 millisecond, and finally spins and sleeps for 50 microseconds.
///
/// The returned multi producer can be cloned to enable distributed writes.
///
/// See: [`DisruptorBuilder`] for configurable disruptor construction.
///
/// # Panics
///
/// If `size` is zero or not a power of two.
///
/// # Examples
/// ```
/// let (multi_producer, consumer) = ansa::mpsc(64, || 0);
/// ```
pub fn mpsc<E>(
    size: usize,
    event_factory: impl FnMut() -> E,
) -> (
    MultiProducer<E, WaitPhased<WaitSleep>, true>,
    Consumer<E, WaitPhased<WaitSleep>>,
)
where
    E: Sync,
{
    let (producer, consumer) = spsc(size, event_factory);
    (producer.into_multi(), consumer)
}

/// Construct a Single-Producer Multi-Consumer disruptor.
///
/// `size` must be a non-zero power of two. `num_consumers` is the number of consumers to create.
/// `event_factory` is used to populate the buffer.
///
/// Uses a [`WaitPhased<WaitSleep>`](WaitPhased) strategy which busy-spins for 1 millisecond, then
/// spins and yields the thread for 1 millisecond, and finally spins and sleeps for 50 microseconds.
///
/// See: [`DisruptorBuilder`] for configurable disruptor construction.
///
/// # Panics
///
/// If `size` is zero or not a power of two.
///
/// # Examples
/// ```
/// let num_consumers = 5;
/// let (producer, consumers) = ansa::spmc(64, num_consumers, || 0);
/// assert_eq!(consumers.len(), 5);
/// ```
#[allow(clippy::type_complexity)]
pub fn spmc<E>(
    size: usize,
    num_consumers: u64,
    event_factory: impl FnMut() -> E,
) -> (
    Producer<E, WaitPhased<WaitSleep>, true>,
    Vec<Consumer<E, WaitPhased<WaitSleep>>>,
)
where
    E: Sync,
{
    assert!(
        size > 0 && (size & (size - 1)) == 0,
        "size ({size}) must be non-zero power of two"
    );
    let mut builder = DisruptorBuilder::new(size, event_factory).wait_strategy(BACKOFF_WAIT);
    for id in 0..num_consumers {
        builder = builder.add_handle(id, Handle::Consumer, Follows::LeadProducer);
    }
    let mut handles = builder.build().unwrap();
    let producer = handles.take_lead().unwrap();
    let consumers = handles.drain_consumers().map(|(_, c)| c).collect();
    (producer, consumers)
}

/// Construct a Multi-Producer Multi-Consumer disruptor.
///
/// `size` must be a non-zero power of two. `num_consumers` is the number of consumers to create.
/// `event_factory` is used to populate the buffer.
///
/// Uses a [`WaitPhased<WaitSleep>`](WaitPhased) strategy which busy-spins for 1 millisecond, then
/// spins and yields the thread for 1 millisecond, and finally spins and sleeps for 50 microseconds.
///
/// The returned multi producer can be cloned to enable distributed writes.
///
/// See: [`DisruptorBuilder`] for configurable disruptor construction.
///
/// # Panics
///
/// If `size` is zero or not a power of two.
///
/// # Examples
/// ```
/// let num_consumers = 5;
/// let (multi_producer, consumers) = ansa::mpmc(64, num_consumers, || 0);
/// assert_eq!(consumers.len(), 5);
/// ```
#[allow(clippy::type_complexity)]
pub fn mpmc<E>(
    size: usize,
    num_consumers: u64,
    event_factory: impl FnMut() -> E,
) -> (
    MultiProducer<E, WaitPhased<WaitSleep>, true>,
    Vec<Consumer<E, WaitPhased<WaitSleep>>>,
)
where
    E: Sync,
{
    let (producer, consumers) = spmc(size, num_consumers, event_factory);
    (producer.into_multi(), consumers)
}
