// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::Result;
use engine_traits::{DBOptions, DBOptionsExt};

impl DBOptionsExt for PanicEngine {
    type DBOptions = PanicDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        panic!()
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicDBOptions;

impl DBOptions for PanicDBOptions {
    fn new() -> Self {
        panic!()
    }

    fn get_max_background_jobs(&self) -> i32 {
        panic!()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        panic!()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        panic!()
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        panic!()
    }

    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()> {
        panic!()
    }
}
