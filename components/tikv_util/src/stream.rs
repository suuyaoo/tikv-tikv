// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use futures::stream::{self, Stream};
use futures_util::io::AsyncRead;
use std::{
    future::Future,
    io, iter,
    marker::Unpin,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::runtime::Builder;

/// Wrapper of an `AsyncRead` instance, exposed as a `Sync` `Stream` of `Bytes`.
pub struct AsyncReadAsSyncStreamOfBytes<R> {
    // we need this Mutex to ensure the type is Sync (provided R is Send).
    // this is because rocksdb::SequentialFile is *not* Sync
    // (according to the documentation it cannot be Sync either,
    // requiring "external synchronization".)
    reader: Mutex<R>,
    // we use this member to ensure every call to `poll_next()` reuse the same
    // buffer.
    buf: Vec<u8>,
}

pub const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

impl<R> AsyncReadAsSyncStreamOfBytes<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: Mutex::new(reader),
            buf: vec![0; READ_BUF_SIZE],
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncReadAsSyncStreamOfBytes<R> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let reader = this.reader.get_mut().expect("lock was poisoned");
        let read_size = Pin::new(reader).poll_read(cx, &mut this.buf);

        match read_size {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(n)) => Poll::Ready(Some(Ok(Bytes::copy_from_slice(&this.buf[..n])))),
        }
    }
}

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
    // we need a Tokio runtime, Tokio futures require Tokio executor.
    Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to create Tokio runtime")
        .block_on(f)
}
