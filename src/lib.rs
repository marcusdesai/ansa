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
