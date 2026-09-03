use anyhow::Result;
use std::sync::OnceLock;
use std::time::Instant;
use tokio::sync::OnceCell;
use wasmtime::Engine;

static WASM_ENGINE: OnceCell<Engine> = OnceCell::const_new();

pub trait WasmEngineSingleton {
    fn global() -> impl std::future::Future<Output = anyhow::Result<&'static wasmtime::Engine>>;
}

impl WasmEngineSingleton for wasmtime::Engine {
    async fn global() -> Result<&'static Engine> {
        WASM_ENGINE
            .get_or_try_init(|| async {
                let mut config = wasmtime::Config::new();
                config.wasm_component_model_async(false);
                config.wasm_component_model(true);

                //config.epoch_interruption(true);

                config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
                config.cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize);
                Engine::new(&config)
                    .map_err(|e| anyhow::anyhow!("Failed to create Wasm engine: {}", e))
            })
            .await
    }
}

static HOST_MONOTONIC_START: OnceLock<Instant> = OnceLock::new();

pub struct GlobalMonotonicClock;

impl wasmtime_wasi::HostMonotonicClock for GlobalMonotonicClock {
    fn resolution(&self) -> u64 {
        1_000_000 // 1ms resolution, safe bet for most systems, (Most systems are ns, even browsers are couple microseconds)
    }

    fn now(&self) -> u64 {
        let start = HOST_MONOTONIC_START.get_or_init(Instant::now);
        start.elapsed().as_nanos() as u64
    }
}
