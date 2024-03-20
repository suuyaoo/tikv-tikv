// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

pub mod cpu_time;
pub mod thread;

#[cfg(target_os = "linux")]
mod cgroup;

// re-export some traits for ease of use
pub use sysinfo::{DiskExt, NetworkExt, ProcessExt, ProcessorExt, SystemExt};

use crate::config::ReadableSize;
use std::sync::Mutex;

lazy_static! {
    pub static ref SYS_INFO: Mutex<sysinfo::System> = Mutex::new(sysinfo::System::new());
}

#[cfg(target_os = "linux")]
pub mod sys_quota {
    use super::super::config::KB;
    use super::{cgroup::CGroupSys, SystemExt, SYS_INFO};

    pub struct SysQuota {
        cgroup: CGroupSys,
    }

    impl SysQuota {
        pub fn new() -> Self {
            Self {
                cgroup: CGroupSys::default(),
            }
        }

        pub fn cpu_cores_quota(&self) -> f64 {
            let cpu_num = num_cpus::get() as f64;
            let quota = match self.cgroup.cpu_cores_quota() {
                Some(cgroup_quota) if cgroup_quota > 0.0 && cgroup_quota < cpu_num => cgroup_quota,
                _ => cpu_num,
            };
            super::limit_cpu_cores_quota_by_env_var(quota)
        }

        pub fn memory_limit_in_bytes(&self) -> u64 {
            let total_mem = {
                let mut system = SYS_INFO.lock().unwrap();
                system.refresh_memory();
                system.get_total_memory() * KB
            };
            let cgroup_memory_limits = self.cgroup.memory_limit_in_bytes();
            if cgroup_memory_limits <= 0 {
                total_mem
            } else {
                std::cmp::min(total_mem, cgroup_memory_limits as u64)
            }
        }

        pub fn log_quota(&self) {
            info!(
                "memory limit in bytes: {}, cpu cores quota: {}",
                self.memory_limit_in_bytes(),
                self.cpu_cores_quota()
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub mod sys_quota {
    use super::super::config::KB;
    use super::{SystemExt, SYS_INFO};

    pub struct SysQuota {}

    impl SysQuota {
        pub fn new() -> Self {
            Self {}
        }

        pub fn cpu_cores_quota(&self) -> f64 {
            let cpu_num = num_cpus::get() as f64;
            super::limit_cpu_cores_quota_by_env_var(cpu_num)
        }

        pub fn memory_limit_in_bytes(&self) -> u64 {
            let mut system = SYS_INFO.lock().unwrap();
            system.refresh_memory();
            system.get_total_memory() * KB
        }

        pub fn log_quota(&self) {
            info!(
                "memory limit in bytes: {}, cpu cores quota: {}",
                self.memory_limit_in_bytes(),
                self.cpu_cores_quota()
            );
        }
    }
}

pub const HIGH_PRI: i32 = -1;

const CPU_CORES_QUOTA_ENV_VAR_KEY: &str = "TIKV_CPU_CORES_QUOTA";

fn limit_cpu_cores_quota_by_env_var(quota: f64) -> f64 {
    match std::env::var(CPU_CORES_QUOTA_ENV_VAR_KEY)
        .ok()
        .and_then(|value| value.parse().ok())
    {
        Some(env_var_quota) if quota.is_sign_positive() => f64::min(quota, env_var_quota),
        _ => quota,
    }
}

fn read_size_in_cache(level: usize, field: &str) -> Option<u64> {
    std::fs::read_to_string(format!(
        "/sys/devices/system/cpu/cpu0/cache/index{}/{}",
        level, field
    ))
    .ok()
    .and_then(|s| s.parse::<ReadableSize>().ok())
    .map(|s| s.0)
}

/// Gets the size of given level cache.
///
/// It will only return `Some` on Linux.
pub fn cache_size(level: usize) -> Option<u64> {
    read_size_in_cache(level, "size")
}

/// Gets the size of given level cache line.
///
/// It will only return `Some` on Linux.
pub fn cache_line_size(level: usize) -> Option<u64> {
    read_size_in_cache(level, "coherency_line_size")
}
