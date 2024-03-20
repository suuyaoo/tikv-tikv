// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::ColumnFamilyOptions;

pub struct PanicColumnFamilyOptions;

impl ColumnFamilyOptions for PanicColumnFamilyOptions {
    fn new() -> Self {
        panic!()
    }
    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_block_cache_capacity(&self) -> u64 {
        panic!()
    }
    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String> {
        panic!()
    }
}
