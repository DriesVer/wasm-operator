
# Callgraph for pkg/controller

This diagram illustrates the initialization flow and the runtime event loop interactions between the Rust host and the WASM modules.

## Initialization & Execution Flow

```mermaid
graph TD
    subgraph "Entry Point (main.rs)"
        Main[main]
        Main -->|Load Metadata| ModMeta[ControllerModuleMetadata::load_modules_from_dir]
        Main -->|Spawn| RtStart[runtime::start]
        Main -->|Send Wrapper| CmdSend[Command::StartModule]
    end

    subgraph "Runtime Orchestration (runtime/mod.rs)"
        RtStart -->|Wait for Command| CmdRecv[ReceiverStream]
        CmdRecv -->|On Command| EnvPre[Environment::cache_precompile]
        CmdRecv -->|Create Module| EnvNew[Environment::new_controller_module]
        CmdRecv -->|Spawn| ModStart[ControllerModule::start]
    end

    subgraph "Controller Module (modules/module.rs)"
        ModStart -->|Init WASM| WasmStart[WasmRuntime::start_controller]
        ModStart -->|Start Loop| EventLoop[run_event_loop]
        EventLoop -->|Poll| PollLoop[poll_event_loop]
        
        PollLoop -->|Check WASM| WasmPoll[WasmRuntime::poll_unpin]
        PollLoop -->|Check Async Ops| ResolveOps[resolve_async_ops]
        PollLoop -->|Check Prediction| PredictionServer[Prediction Logic]
        
        ResolveOps -->|Lock| OpsRunner
        ResolveOps -->|If Result Ready| WasmWake[WasmRuntime::wakeup]
    end

    subgraph "ABI & Host Interface (abi/mod.rs)"
        WasmGuest[WASM Guest Code] -->|Import| AbiReq[abi_request]
        WasmGuest -->|Import| AbiDelay[abi_delay]
        
        AbiReq -->|Register Op| OpsRunner[OpsRunner::handle_request]
        AbiDelay -->|Register Op| OpsRunner
    end

    CmdSend -.->|Channel| CmdRecv
    WasmPoll -.->|Executes| WasmGuest
```

## detailed Interaction Loop

1. **Bootstrap**: `main` initializes the runtime and sends a `StartModule` command.
2. **Instantiation**: `runtime::start` receives the command, compiles the WASM (if needed), and creates a `ControllerModule`.
3. **Execution**: `ControllerModule::start` kicks off the `_start` function in WASM.
4. **Event Loop**:
    - The module polls the WASM instance.
    - If the WASM guest needs to perform an I/O operation (like an HTTP request), it calls the host function via ABI (`abi_request`).
    - The host registers this operation in `OpsRunner`.
    - The `ControllerModule`'s `resolve_async_ops` checks for completed operations.
    - When an operation completes, `WasmRuntime::wakeup` is called to notify the WASM guest with the result.
