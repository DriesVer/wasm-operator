# Setting Up the WASM-Operator

This guide walks you through setting up the WASM-operator using the project's Development CLI. You can also perform these steps manually if your cluster requires a custom configuration.

To get started, load the Development CLI helper script by sourcing it from the root of the repository:

```sh
source ./devel/cli.sh
```

## Cloning the Repository

The repository relies on submodules. Clone it with the `--recurse-submodules` flag to ensure all nested components are checked out:

```sh
git clone --recurse-submodules git@github.com:DriesVer/wasm-operator.git
```

If you already cloned the repository without submodules, initialize and update them by running:

```sh
git submodule update --init --recursive
```

## Dependencies

Make sure the following tools are installed on your host:

- Kind
- Kubectl
- Docker
- Rust
- Cargo
- Cargo Zigbuild (required on macOS)

Verify your development environment with the CLI check tool:

```sh
wasmop check
```

## Cluster Setup

For developing the WasmOperator framework or building child operators, we recommend a local Kind cluster. Start the interactive configuration wizard:

```sh
wasmop setup
```

The interactive prompt will guide you through:

- **Creating a Kind cluster (recommended):** Prompts for the cluster name.
- **Directory mounting:** Mounts a host directory into the cluster. The `wasmop build child` command places compiled child operators here so the cluster can read them. You can use the local `./tests/wasm_source_dir` path.
- **Installing WasmOperator CRD:** Applies the `WasmOperator` Custom Resource Definition to either the newly created Kind cluster or your current kubectl context. This step uses a helper Rust script which will automatically be compiled and executed.

For this tutorial, enter the following values when prompted:

- **Cluster name:** `wasmoperator`
- **Mount a local directory:** `yes`
- **Mount local directory:** `[Absolute path]/tests/wasm_source_dir`
- **Container mount path:** `/mnt/host`

> **Important:** If you mount a local host directory, ensure it is world-writable. The parent operator runs as a non-root user and needs write access:

```sh
chmod 777 /tests/wasm_source_dir
```

## Parent Controller Setup

Install the target for `unknown-linux-musl` matching your system architecture:

```sh
rustup target add x86_64-unknown-linux-musl
```

```sh
rustup target add aarch64-unknown-linux-musl
```

Next, ensure the musl development libraries and compiler toolchain are available. On Ubuntu/Debian, install them via `apt`:

```sh
sudo apt install -y musl musl-dev musl-tools build-essential
```

Build and load the parent controller into the cluster:

```sh
wasmop load parent wasmoperator
```

This command creates a controller pod that contains both the parent operator and a prediction webserver sidecar. It also provisions the necessary ServiceAccount, applies required RBAC rules, and binds the host directory volume.

Verify that the controller started cleanly:

```sh
kubectl logs -f controller
```

The output should resemble:

```
Defaulted container "controller" out of: controller, prediction-sidecar
2026-09-03T02:34:54.197029Z  INFO controller: Logging initialized with base level: debug
2026-09-03T02:34:54.197204Z  INFO controller: HTTP logging level set to: info
2026-09-03T02:34:54.197213Z  INFO controller: Kubernetes logging level set to: info
2026-09-03T02:34:54.197257Z  INFO controller: Detected 4 CPU cores, using 4 worker threads for the async runtime.
2026-09-03T02:34:54.258048Z DEBUG controller::runtime: Starting idle check loop with inactive threshold 5s and idle threshold 500ms
```

## Deploying Your First Child Operator

Next, deploy the sample `simple-rust-operator`. This operator watches for `TestResource` custom resources and multiplies the resource's `factor` value by an internal execution counter, storing the result in the `outcome` field.

Navigate to the example directory:

```sh
cd examples/simple-rust-operator/
```

Apply the `TestResource` CRD and grant the parent controller permissions to reconcile it:

```sh
kubectl apply -f test-manifests/TestResource_CRD.yaml
kubectl apply -f test-manifests/rbac.yaml
```

Compile and register the child operator:

```sh
wasmop load child simple_rust_operator
```

This compiles the child operator to the `wasm32-wasip2` target, copies the WebAssembly code into the shared mount directory, and creates a corresponding `WasmOperator` custom resource. The parent operator will automatically detect the new resource and load the module.

Check the parent controller logs to confirm the module loaded. Once ready, test reconciliation by creating a `TestResource`:

```sh
kubectl apply -f test-manifests/function1.yaml
kubectl get testresource
```

The output should confirm the calculated result:

```
NAME        FACTOR   OUTCOME   LAST UPDATED
function1   1        1         84s
```

Test state persistence by applying a second resource with a different factor. The child operator multiplies the factor by its internal counter (now `2`):

```
NAME        FACTOR   OUTCOME   LAST UPDATED
function1   1        1         2m13s
function2   10       20        1s
```

## Deploying Parent and Child Operators Concurrently

To build and load both the parent controller and child operator in a single step, combine the commands:

```sh
wasmop load wasmoperator simple_rust_operator
```

If previous versions are active, this stops them and deploys the new builds. Other running child operators remain scheduled, though any in-memory state will reset when the parent operator restarts.
