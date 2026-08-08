use anyhow::Result;
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
