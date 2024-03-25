// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! To use External storage with protobufs as an application, import this module.
//! external_storage contains the actual library code
//! Cloud provider backends are under components/cloud
use std::io::{self};
use std::path::Path;

pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;

use external_storage::{record_storage_create, BackendConfig};
pub use external_storage::{
    read_external_storage_into_file, ExternalStorage, LocalStorage, NoopStorage, UnpinReader,
};
use kvproto::brpb::{Noop, StorageBackend};
use tikv_util::time::Instant;

pub fn create_storage(
    storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend(backend, config)
    } else {
        Err(bad_storage_backend(storage_backend))
    }
}

fn bad_storage_backend(storage_backend: &StorageBackend) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("bad storage backend {:?}", storage_backend),
    )
}

fn bad_backend(backend: Backend) -> io::Error {
    let storage_backend = StorageBackend {
        backend: Some(backend),
        ..Default::default()
    };
    bad_storage_backend(&storage_backend)
}

pub fn create_backend(
    backend: &Backend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    create_backend_inner(backend, config)
}

/// Create a new storage from the given storage backend description.
fn create_backend_inner(
    backend: &Backend,
    _backend_config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    let start = Instant::now();
    let storage: Box<dyn ExternalStorage> = match backend {
        Backend::Local(local) => {
            let p = Path::new(&local.path);
            Box::new(LocalStorage::new(p)?) as Box<dyn ExternalStorage>
        }
        Backend::Noop(_) => Box::new(NoopStorage::default()) as Box<dyn ExternalStorage>,
        #[allow(unreachable_patterns)]
        _ => return Err(bad_backend(backend.clone())),
    };
    record_storage_create(start, &*storage);
    Ok(storage)
}

pub fn make_local_backend(path: &Path) -> StorageBackend {
    let path = path.display().to_string();
    let mut backend = StorageBackend::default();
    backend.mut_local().set_path(path);
    backend
}

/// Creates a noop `StorageBackend`.
pub fn make_noop_backend() -> StorageBackend {
    let noop = Noop::default();
    let mut backend = StorageBackend::default();
    backend.set_noop(noop);
    backend
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
        match create_storage(&backend, Default::default()) {
            Ok(_) => panic!("must be NotFound error"),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
        }

        let backend = make_local_backend(path);
        create_storage(&backend, Default::default()).unwrap();

        let backend = make_noop_backend();
        create_storage(&backend, Default::default()).unwrap();

        let backend = StorageBackend::default();
        assert!(create_storage(&backend, Default::default()).is_err());
    }
}
