# Parent Controller Environment Variables

The parent controller (located in `pkg/controller`) can be configured via environment variables. These variables are divided into two categories: **Compile-time** and **Runtime**.

## Compile-time Environment Variables

These variables are evaluated when the parent controller is built. If you need to change their values, you must recompile the `controller` binary.

- `WASMOP_CACHE_DIR`

  - **Description**: Directory where the parent controller stores its cached WebAssembly modules.
  - **Default**: `/tmp/wasmop-cache`

- `WASMOP_IDLE_THRESHOLD`

  - **Description**: The time duration an operator can remain idle before the controller unloads it from memory to save resources. Can be specified in milliseconds (e.g. `5000`), seconds (e.g. `5s`), minutes (e.g. `5m`), hours (`h`), or days (`d`).
  - **Default**: `5000` (5000 ms / 5 seconds)

- `WASMOP_EXECUTE_THRESHOLD`

  - **Description**: The minimum execution duration after a watch event (except bookmark). This threshold is used in conjunction with the idle threshold. If the operator's execution time is below this threshold, it has not been executed enough to be considered possibly idle.
    Note that `WASMOP_IDLE_THRESHOLD` must be greater than `WASMOP_EXECUTE_THRESHOLD`.
  - **Default**: `500` (500 ms)

- `WASMOP_USE_RECONCILE_PREDICTION`

  - **Description**: Flag to enable the reconcile prediction feature. If enabled, the controller uses an autoregressive model based on the operator's reconcile history to proactively load the module before the next expected reconciliation event.
  - **Values**: `true` or `1` to enable, otherwise disabled.
  - **Default**: Disabled

- `WASMOP_ERROR_LOG_SIZE`
  - **Description**: The maximum number of entries to keep in the in-memory FIFO buffer for recent operator errors. This affects the statistics and error logs tracked per operator.
  - **Default**: `10`

## Runtime Environment Variables

These variables are evaluated when the parent controller starts and can be passed to the Docker container or execution environment directly.

- `RUST_LOG`

  - **Description**: Configures the logging level for the controller using the standard `tracing-subscriber` env filter syntax (e.g., `info`, `debug`, or module-specific like `kube=debug`). It overrides default internal logging parameters.
  - **Default**: Inherits application defaults based on CLI arguments.

- `WASMOP_NAMESPACE`
  - **Description**: The Kubernetes namespace that the controller will monitor for `WasmOperator` custom resources.
  - **Default**: `default`
