//! Filesystem source and sink for tar archives.
//!
//! This module is available only on Unix, with the `fs` feature.

#[cfg_attr(not(test), expect(dead_code))]
mod rt;
mod sys;
