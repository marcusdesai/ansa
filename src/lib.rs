//! MPMC queue implementation using the disruptor pattern
//! todo
//!
//! to mention:
//! - no panics
//! - sequence limit (i64::MAX)
//! - Basics of how the disruptor works
//! - traffic jam analogy
//! - features
//! - examples
//! - terminology (ring buffer, handle, etc...)
//!
//! A disruptor is made up of:
//! - A ring buffer (also called a circular buffer)
//!

mod builder;
mod handles;
mod ringbuffer;
pub mod wait;

pub use builder::*;
pub use handles::*;

use wait::WaitSleep;

/// Construct a Single Producer Single Consumer disruptor.
///
/// `size` must be a non-zero power of two. `event_factory` is used to populate the buffer.
///
/// # Panics
///
/// If size is zero or not a power of two.
///
/// # Examples
/// ```
/// let (producer, consumer) = ansa::spsc(64, || 0);
/// ```
pub fn spsc<E, F>(
    size: usize,
    event_factory: F,
) -> (Producer<E, WaitSleep, true>, Consumer<E, WaitSleep>)
where
    E: Sync,
    F: FnMut() -> E,
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

/// Construct a Multi Producer Single Consumer disruptor.
///
/// `size` must be a non-zero power of two. `event_factory` is used to populate the buffer.
///
/// The returned producer may be cloned to allow distributed writes.
///
/// # Panics
///
/// If size is zero or not a power of two.
///
/// # Examples
/// ```
/// let (producer, consumer) = ansa::mpsc(64, || 0);
///
/// assert!(matches!(producer, ansa::MultiProducer { .. }));
/// ```
pub fn mpsc<E, F>(
    size: usize,
    event_factory: F,
) -> (MultiProducer<E, WaitSleep, true>, Consumer<E, WaitSleep>)
where
    E: Sync,
    F: FnMut() -> E,
{
    let (producer, consumer) = spsc(size, event_factory);
    (producer.into_multi(), consumer)
}

/// Construct a Single Producer Multi Consumer disruptor.
///
/// `size` must be a non-zero power of two. `num_consumers` is the number of consumers to create,
/// which cannot be changed after this point. `event_factory` is used to populate the buffer.
///
/// # Panics
///
/// If size is zero or not a power of two.
///
/// # Examples
/// ```
/// let num_consumers = 5;
/// let (producer, consumers) = ansa::spmc(64, num_consumers, || 0);
///
/// assert_eq!(consumers.len(), 5);
/// ```
pub fn spmc<E, F>(
    size: usize,
    num_consumers: u64,
    event_factory: F,
) -> (Producer<E, WaitSleep, true>, Vec<Consumer<E, WaitSleep>>)
where
    E: Sync,
    F: FnMut() -> E,
{
    assert!(
        size > 0 && (size & (size - 1)) == 0,
        "size ({size}) must be non-zero power of two"
    );
    let mut builder = DisruptorBuilder::new(size, event_factory);
    for id in 0..num_consumers {
        builder = builder.add_handle(id, Handle::Consumer, Follows::LeadProducer);
    }
    let mut handles = builder.build().unwrap();
    let producer = handles.take_lead().unwrap();
    let consumers = handles.drain_consumers().map(|(_, c)| c).collect();
    (producer, consumers)
}

/// Construct a Multi Producer Multi Consumer disruptor.
///
/// `size` must be a non-zero power of two. `num_consumers` is the number of consumers to create,
/// which cannot be changed after this point. `event_factory` is used to populate the buffer.
///
/// The returned producer may be cloned to allow distributed writes.
///
/// # Panics
///
/// If size is zero or not a power of two.
///
/// # Examples
/// ```
/// let num_consumers = 5;
/// let (producer, consumers) = ansa::mpmc(64, num_consumers, || 0);
///
/// assert_eq!(consumers.len(), 5);
/// assert!(matches!(producer, ansa::MultiProducer { .. }));
/// ```
pub fn mpmc<E, F>(
    size: usize,
    num_consumers: u64,
    event_factory: F,
) -> (
    MultiProducer<E, WaitSleep, true>,
    Vec<Consumer<E, WaitSleep>>,
)
where
    E: Sync,
    F: FnMut() -> E,
{
    let (producer, consumers) = spmc(size, num_consumers, event_factory);
    (producer.into_multi(), consumers)
}
