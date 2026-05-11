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
                config.async_support(true);
                config.cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize);
                Engine::new(&config)
                    .map_err(|e| anyhow::anyhow!("Failed to create Wasm engine: {}", e))
            })
            .await
    }
}
