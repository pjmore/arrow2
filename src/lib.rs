#![cfg_attr(not(feature="std"), no_std)]
#[macro_use]
extern crate alloc;



mod arrow_alloc;
pub mod array;
pub mod bitmap;
pub mod buffer;
mod endianess;
pub mod error;
pub mod trusted_len;
pub mod types;

pub mod compute;
pub mod io;
pub mod record_batch;
pub mod temporal_conversions;
pub use arrow_alloc::total_allocated_bytes;

pub mod datatypes;

pub mod ffi;
pub mod util;

// so that documentation gets test
#[cfg(any(test, doctest))]
mod docs;
