// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use futures::stream::{self, Stream};
use std::{
    future::Future,
    io, iter,
    marker::Unpin,
};
use tokio::runtime::Builder;

pub fn error_stream(e: io::Error) -> impl Stream<Item = io::Result<Bytes>> + Unpin + Send + Sync {
    stream::iter(iter::once(Err(e)))
}

/// Runs a future on the current thread involving external storage.
///
/// # Caveat
///
/// This function must never be nested. The future invoked by
/// `block_on_external_io` must never call `block_on_external_io` again itself,
/// otherwise the executor's states may be disrupted.
///
/// This means the future must only use async functions.
// FIXME: get rid of this function, so that futures_executor::block_on is sufficient.
pub fn block_on_external_io<F: Future>(f: F) -> F::Output {
    // we need a Tokio runtime rather than futures_executor::block_on because
    // Tokio futures require Tokio executor.
    Builder::new()
        .basic_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to create Tokio runtime")
        .block_on(f)
}

/// Trait for errors which can be retried inside [`retry()`].
pub trait RetryError {
    /// Returns a placeholder to indicate an uninitialized error. This function exists only to
    /// satisfy safety, there is no meaning attached to the returned value.
    fn placeholder() -> Self;

    /// Returns whether this error can be retried.
    fn is_retryable(&self) -> bool;
}

