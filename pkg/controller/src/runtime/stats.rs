use chrono::{SecondsFormat, Utc};
use std::cmp::min;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU16, AtomicU32, AtomicU64, Ordering};
use tokio::sync::Mutex;

use crate::kubernetes::crd::WasmOperatorStatistics;

// TODO: maybe use prometheus but prometheus can be more resource intensive

struct AsyncFixedBuffer<T> {
    data: Mutex<VecDeque<T>>,
    limit: usize,
}

impl<T> AsyncFixedBuffer<T> {
    fn new(limit: usize) -> Self {
        Self {
            data: Mutex::new(VecDeque::with_capacity(limit)),
            limit,
        }
    }

    async fn push(&self, item: T) {
        let mut data = self.data.lock().await;
        if data.len() >= self.limit {
            data.pop_back();
        }
        data.push_front(item);
    }

    async fn get_all(&self) -> Vec<T>
    where
        T: Clone,
    {
        let data = self.data.lock().await;
        data.iter().cloned().collect()
    }
}

pub struct WasmOperatorStatisticsRecorder {
    last_reconcile: AtomicI64,   // Unix timestamp in hours of last reconcile
    reconciles: [AtomicU16; 24], // Number of reconciles per hour (0-23) (max ~65000 reconciles per hour)

    reconcile_total: AtomicU64,
    reconcile_total_duration_ms: AtomicU64,
    reconcile_max_duration_ms: AtomicU32, // Max 49 days

    load_unload_op_total: AtomicU64,
    last_load_op: AtomicI64, // Unix timestamp in seconds of last load operation

    load_total_duration_ms: AtomicU64,
    load_max_duration_ms: AtomicU32,

    idle_total_duration_s: AtomicU64,
    idle_max_duration_s: AtomicU64,
    active_total_duration_s: AtomicU64,
    active_max_duration_s: AtomicU64,

    memory_usage_bytes: AtomicU32, // Max 4GB

    error_log: AsyncFixedBuffer<String>,
}

impl WasmOperatorStatisticsRecorder {
    pub fn new() -> Self {
        let err_buffer_size = option_env!("WASMOP_ERROR_LOG_SIZE")
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        Self {
            last_reconcile: AtomicI64::new(0),
            reconciles: Default::default(),
            reconcile_total: AtomicU64::new(0),
            reconcile_total_duration_ms: AtomicU64::new(0),
            reconcile_max_duration_ms: AtomicU32::new(0),
            load_unload_op_total: AtomicU64::new(0),
            last_load_op: AtomicI64::new(0),
            load_total_duration_ms: AtomicU64::new(0),
            load_max_duration_ms: AtomicU32::new(0),
            idle_total_duration_s: AtomicU64::new(0),
            idle_max_duration_s: AtomicU64::new(0),
            active_total_duration_s: AtomicU64::new(0),
            active_max_duration_s: AtomicU64::new(0),
            memory_usage_bytes: AtomicU32::new(0),
            error_log: AsyncFixedBuffer::new(err_buffer_size),
        }
    }

    pub fn record_reconcile(&self, duration_ms: u32) {
        let now = Utc::now().timestamp();
        let last_reconcile = self.last_reconcile.load(Ordering::Relaxed);
        self.last_reconcile.store(now, Ordering::Relaxed);

        let now = now / 3600;
        let last_reconcile = last_reconcile / 3600;

        // Clear old reconcile counts
        let diff = now - last_reconcile;
        if diff > 0 || now != last_reconcile {
            // There was been some time since last reconcile or we moved to a different hour
            let diff_hours = min(diff, 23);

            for h in 0..=diff_hours {
                let reset_hour = (last_reconcile + h + 1) % 24;
                self.reconciles[reset_hour as usize].store(0, Ordering::Relaxed);
            }
        }

        // Update the reconcile count/duration stats
        self.reconciles[(now % 24) as usize].fetch_add(1, Ordering::Relaxed);
        self.reconcile_total.fetch_add(1, Ordering::Relaxed);
        self.reconcile_total_duration_ms
            .fetch_add(duration_ms as u64, Ordering::Relaxed);
        self.reconcile_max_duration_ms
            .fetch_max(duration_ms, Ordering::Relaxed);
    }

    pub fn record_load_duration(&self, duration_ms: u32) {
        self.load_total_duration_ms
            .fetch_add(duration_ms as u64, Ordering::Relaxed);
        self.load_max_duration_ms
            .fetch_max(duration_ms, Ordering::Relaxed);
    }

    pub fn record_memory_usage(&self, bytes: u32) {
        self.memory_usage_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn record_loading_operation(&self) {
        self.load_unload_op_total.fetch_add(1, Ordering::Relaxed);

        let now = Utc::now().timestamp();
        let last_load_op = self.last_load_op.swap(now, Ordering::Relaxed);

        if last_load_op > 0 {
            let duration_since_last_load_op = now - last_load_op;
            self.idle_total_duration_s
                .fetch_add(duration_since_last_load_op as u64, Ordering::Relaxed);
            self.idle_max_duration_s
                .fetch_max(duration_since_last_load_op as u64, Ordering::Relaxed);
        }
    }

    pub fn record_unloading_operation(&self) {
        self.load_unload_op_total.fetch_add(1, Ordering::Relaxed);

        let now = Utc::now().timestamp();
        let last_load_op = self.last_load_op.swap(now, Ordering::Relaxed);

        if last_load_op > 0 {
            let duration_since_last_load_op = now - last_load_op;
            self.active_total_duration_s
                .fetch_add(duration_since_last_load_op as u64, Ordering::Relaxed);
            self.active_max_duration_s
                .fetch_max(duration_since_last_load_op as u64, Ordering::Relaxed);
        }
    }

    pub async fn record_error(&self, error: &str) {
        self.error_log
            .push(format!(
                "[{}] {}",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                error
            ))
            .await;
    }

    fn get_reconcile_total_24h(&self) -> u32 {
        self.reconciles.iter().fold(0, |acc, hour_count| {
            acc + hour_count.load(Ordering::Relaxed) as u32
        })
    }

    fn get_load_total(&self) -> u64 {
        // The load counter is represented in the odd state of the load/unload counter
        // We use a ceil to get the total number of load operations
        (self.load_unload_op_total.load(Ordering::Relaxed) + 1) / 2
    }

    fn get_unload_total(&self) -> u64 {
        // The unload counter is represented in the even state of the load/unload counter
        // We use a floor to get the total number of unload operations
        self.load_unload_op_total.load(Ordering::Relaxed) / 2
    }

    fn get_reconcile_cold_start_ratio(&self) -> u8 {
        let total_reconciles = self.reconcile_total.load(Ordering::Relaxed);
        if total_reconciles == 0 {
            return 0;
        }
        let cold_executions = self.get_load_total();
        ((cold_executions * 100) / total_reconciles) as u8
    }

    fn get_wasm_load_duration_msec_avg(&self) -> u32 {
        let load_total = self.get_load_total();
        if load_total == 0 {
            return 0;
        }
        (self.load_total_duration_ms.load(Ordering::Relaxed) / load_total) as u32
    }

    fn get_reconcile_duration_msec_avg(&self) -> u32 {
        let reconcile_total = self.reconcile_total.load(Ordering::Relaxed);
        if reconcile_total == 0 {
            return 0;
        }
        (self.reconcile_total_duration_ms.load(Ordering::Relaxed) / reconcile_total) as u32
    }

    fn get_activity_ratio(&self) -> u8 {
        let idle_duration = self.idle_total_duration_s.load(Ordering::Relaxed);
        let running_duration = self.active_total_duration_s.load(Ordering::Relaxed);
        let total_duration = idle_duration + running_duration;
        if total_duration == 0 {
            return 0;
        }
        ((running_duration * 100) / total_duration) as u8
    }

    fn get_idle_duration_sec_avg(&self) -> u64 {
        let unload_total = self.get_unload_total();
        if unload_total == 0 {
            return 0;
        }
        self.idle_total_duration_s.load(Ordering::Relaxed) / unload_total
    }

    fn get_running_duration_s_avg(&self) -> u64 {
        let load_total = self.get_load_total();
        if load_total == 0 {
            return 0;
        }
        self.active_total_duration_s.load(Ordering::Relaxed) / load_total
    }

    pub async fn get_statistics(&self) -> WasmOperatorStatistics {
        WasmOperatorStatistics {
            reconcile_total_24h: self.get_reconcile_total_24h(),
            reconcile_cold_start_ratio: self.get_reconcile_cold_start_ratio(),
            wasm_load_duration_msec_avg: self.get_wasm_load_duration_msec_avg(),
            wasm_load_duration_msec_max: self.load_max_duration_ms.load(Ordering::Relaxed),
            reconcile_duration_msec_avg: self.get_reconcile_duration_msec_avg(),
            reconcile_duration_msec_max: self.reconcile_max_duration_ms.load(Ordering::Relaxed),
            memory_usage_bytes: self.memory_usage_bytes.load(Ordering::Relaxed),
            activity_ratio: self.get_activity_ratio(),
            idle_duration_sec_avg: self.get_idle_duration_sec_avg(),
            idle_duration_sec_max: self.idle_max_duration_s.load(Ordering::Relaxed),
            active_duration_sec_avg: self.get_running_duration_s_avg(),
            active_duration_sec_max: self.active_max_duration_s.load(Ordering::Relaxed),
            recent_errors: self.error_log.get_all().await,
        }
    }
}
