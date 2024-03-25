// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::engine::RocksEngine;
use crate::options::RocksWriteOptions;
use crate::util::get_cf_handle;
use engine_traits::{self, Error, Mutable, Result, WriteBatchExt, WriteOptions};
use rocksdb::{Writable, WriteBatch as RawWriteBatch, DB};

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        Self::WriteBatch::new(Arc::clone(self.as_inner()))
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        Self::WriteBatch::with_capacity(Arc::clone(self.as_inner()), cap)
    }
}

pub struct RocksWriteBatch {
    db: Arc<DB>,
    wb: RawWriteBatch,
}

impl RocksWriteBatch {
    pub fn new(db: Arc<DB>) -> RocksWriteBatch {
        RocksWriteBatch {
            db,
            wb: RawWriteBatch::default(),
        }
    }

    pub fn as_inner(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn with_capacity(db: Arc<DB>, cap: usize) -> RocksWriteBatch {
        let wb = if cap == 0 {
            RawWriteBatch::default()
        } else {
            RawWriteBatch::with_capacity(cap)
        };
        RocksWriteBatch { db, wb }
    }

    pub fn from_raw(db: Arc<DB>, wb: RawWriteBatch) -> RocksWriteBatch {
        RocksWriteBatch { db, wb }
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    pub fn merge(&mut self, src: &Self) {
        self.wb.append(src.wb.data());
    }
}

impl engine_traits::WriteBatch<RocksEngine> for RocksWriteBatch {
    fn with_capacity(e: &RocksEngine, cap: usize) -> RocksWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
        let opt: RocksWriteOptions = opts.into();
        self.get_db()
            .write_opt(self.as_inner(), &opt.into_raw())
            .map_err(Error::Engine)
    }

    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.wb.count() > RocksEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        self.wb.clear();
    }

    fn set_save_point(&mut self) {
        self.wb.set_save_point();
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.wb.pop_save_point().map_err(Error::Engine)
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.wb.rollback_to_save_point().map_err(Error::Engine)
    }

    fn merge(&mut self, src: Self) {
        self.wb.append(src.wb.data());
    }
}

impl Mutable for RocksWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wb.delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.delete_cf(handle, key).map_err(Error::Engine)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.wb
            .delete_range(begin_key, end_key)
            .map_err(Error::Engine)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

