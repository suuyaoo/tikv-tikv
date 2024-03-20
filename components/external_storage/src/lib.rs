// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! External storage support.
//!
//! This crate define an abstraction of external storage. Currently, it
//! supports local storage.

#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;
extern crate fail;

use std::io;
use std::marker::Unpin;
use std::path::Path;
use std::sync::Arc;

use futures_io::AsyncRead;
#[cfg(feature = "protobuf-codec")]
use kvproto::backup::StorageBackend_oneof_backend as Backend;
#[cfg(feature = "prost-codec")]
use kvproto::backup::{storage_backend::Backend, Local};
use kvproto::backup::{Noop, StorageBackend};
use tikv_util::time::Instant;

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod util;
pub use util::block_on_external_io;
mod metrics;
use metrics::*;

pub const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

/// Create a new storage from the given storage backend description.
pub fn create_storage(backend: &StorageBackend) -> io::Result<Arc<dyn ExternalStorage>> {
    let start = Instant::now();
    let (label, storage) = match &backend.backend {
        Some(Backend::Local(local)) => {
            let p = Path::new(&local.path);
            ("local", LocalStorage::new(p).map(|s| Arc::new(s) as _))
        }
        Some(Backend::Noop(_)) => ("noop", Ok(Arc::new(NoopStorage::default()) as _)),
        _ => {
            let u = url_of_backend(backend);
            error!("unknown storage"; "scheme" => u.scheme());
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown storage {}", u),
            ));
        }
    };
    EXT_STORAGE_CREATE_HISTOGRAM
        .with_label_values(&[label])
        .observe(start.saturating_elapsed().as_secs_f64());
    storage
}

/// Formats the storage backend as a URL.
pub fn url_of_backend(backend: &StorageBackend) -> url::Url {
    let mut u = url::Url::parse("unknown:///").unwrap();
    match &backend.backend {
        Some(Backend::Local(local)) => {
            u.set_scheme("local").unwrap();
            u.set_path(&local.path);
        }
        Some(Backend::Noop(_)) => {
            u.set_scheme("noop").unwrap();
        }
        None => {}
    }
    u
}

/// Creates a local `StorageBackend` to the given path.
pub fn make_local_backend(path: &Path) -> StorageBackend {
    let path = path.display().to_string();
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::Local(Local { path })),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.mut_local().set_path(path);
        backend
    }
}

/// Creates a noop `StorageBackend`.
pub fn make_noop_backend() -> StorageBackend {
    let noop = Noop::default();
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::Noop(noop)),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.set_noop(noop);
        backend
    }
}

/// An abstraction of an external storage.
// TODO: these should all be returning a future (i.e. async fn).
pub trait ExternalStorage: 'static {
    /// Write all contents of the read to the given path.
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()>;
    /// Read all contents of the given path.
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_>;
}

impl ExternalStorage for Arc<dyn ExternalStorage> {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        (**self).write(name, reader, content_length)
    }
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).read(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_create_storage() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let backend = make_local_backend(&path.join("not_exist"));
        match create_storage(&backend) {
            Ok(_) => panic!("must be NotFound error"),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
        }

        let backend = make_local_backend(path);
        create_storage(&backend).unwrap();

        let backend = make_noop_backend();
        create_storage(&backend).unwrap();

        let backend = StorageBackend::default();
        assert!(create_storage(&backend).is_err());
    }

    #[test]
    fn test_url_of_backend() {
        let backend = make_local_backend(Path::new("/tmp/a"));
        assert_eq!(url_of_backend(&backend).to_string(), "local:///tmp/a");

        let backend = make_noop_backend();
        assert_eq!(url_of_backend(&backend).to_string(), "noop:///");
    }
}
