# WasmOperator Custom Resource Definition (CRD)

The `WasmOperator` custom resource defines a WebAssembly-based child operator managed by the parent controller in the wasm-operator framework.

This document explains the available fields within the `WasmOperator` specification (`spec`) and the information exposed in its `status`.

The WasmOperator CRD manifest can be created by running a helper Rust script.
Follow the [Setup documentation](./setup.md) to learn how to generate and apply the CRD manifest to your Kubernetes cluster.

## Specification (`spec`)

The `spec` section dictates how the parent controller should configure and execute the WebAssembly module.

### `wasm` (Required)

Defines the source location of the compiled WebAssembly (`.wasm`) module.

- **Type**: Object (`WasmSource`)
- **Supported Option**:
  - `pvc`: Loads the module from a persistent volume or mounted directory accessible to the parent controller.
    - `path`: The directory path containing the file.
    - `file`: The exact filename of the WebAssembly module (e.g., `simple_rust_operator.wasm`).

### `env` (Optional)

An array of environment variables to inject into the WebAssembly runtime environment.

- **Type**: Array of Objects
- **Fields**:
  - `name`: The name of the environment variable.
  - `value`: The string value of the environment variable.

### `args` (Optional)

A list of command-line arguments to pass to the WebAssembly module upon execution.

- **Type**: Array of Strings

---

## Status (`status`)

The `status` section is automatically updated by the parent controller to report the current operational state and runtime metrics of the child operator.

### Core Status Fields

- **`state`**: The current lifecycle state of the operator.
  - _Values_: `Unclaimed`, `Running`, `Idle`, `Paused`, `Error`
- **`lastUpdated`**: Timestamp (`DateTime`) of the last time the status was updated.
- **`observedGeneration`**: The generation of the custom resource specification that the controller last processed.
- **`owner`**: The identifier of the parent controller instance that has claimed and is currently managing this operator.

### Statistics (`statistics`)

Detailed runtime metrics tracked by the parent controller for the specific Wasm module.

- **`reconcileTotal24h`**: Total number of reconciliation events handled by the operator in the last 24 hours.
- **`reconcileColdStartRatio`**: A ratio indicating how often the Wasm module had to be cold-started (loaded from disk/cache into memory) to perform a reconcile. Values above 100 mean the module was predictively loaded more often than it actually reconciled.
- **`wasmLoadDurationMsecAvg`**: Average time taken (in milliseconds) to instantiate the WebAssembly module.
- **`wasmLoadDurationMsecMax`**: Maximum time taken (in milliseconds) to instantiate the WebAssembly module.
- **`memoryUsageBytes`**: Current memory consumed by the running WebAssembly instance (if loaded).
- **`activityRatio`**: Percentage (0-100) representing how much time the module spends actively executing versus sitting idle.
- **`idleDurationSecAvg` / `idleDurationSecMax`**: The average and maximum duration (in seconds) the module remained loaded in memory while idle.
- **`activeDurationSecAvg` / `activeDurationSecMax`**: The average and maximum duration (in seconds) the module spent actively processing events.
- **`recentErrors`**: A list of recent error messages encountered by the operator during execution or initialization.
