# WasmOperator Development CLI (`wasmop`)

The `wasmop` Development CLI provides developer tooling to build, test, configure, and deploy WebAssembly-based Kubernetes operators alongside their host parent controller.

To use this CLI, you must first source the script in your Bash or Zsh shell session before executing any commands:

```sh
source cli.sh
```

Commands can be executed with the `wasmop` prefix.

```sh
wasmop <command> [subcommand] [arguments...]
```

The commands only use positional arguments, and the order of arguments is significant. Optional arguments are indicated in square brackets (`[ ]`), while required arguments are indicated in angle brackets (`< >`).

The most common commands are :

- `wasmop test` to verify that the CLI is correctly sourced and operational.
- `wasmop setup` to run the full initial setup workflow.
- `wasmop build` to compile and package the parent controller and child operator. (Optionally use subcommands `parent` and `child` to build them separately.)
- `wasmop load` to deploy the parent controller and child operator into a Kind cluster. (Optionally use subcommands `parent` and `child` to build them separately.)

---

### wasmop test

Verifies that the CLI dispatcher is correctly sourced and operational by printing a confirmation message.

```sh
wasmop test
```

---

### wasmop config list

Displays all environment variables currently saved in the local configuration file (`wasmop_config.sh`).

```sh
wasmop config list
```

---

### wasmop config savevar <var_name> <var_value>

Persists or updates a key-value configuration variable inside the `wasmop_config.sh` file.
This command is normally invoked automatically by other CLI commands that require configuration variables, but it can also be used directly to manually set or update a variable.

```sh
wasmop config savevar <var_name> <var_value>
```

- `<var_name>`: The name of the configuration variable to save (e.g., `WASMOP_HOST_FOLDER`).
- `<var_value>`: The value assigned to the configuration variable.

---

### wasmop check install

Checks the host system for required dependencies (`kind`, `kubectl`, `docker`, `rustc`, `cargo`, and `cargo-zigbuild` on macOS/Darwin) and prints their installation status and versions.

```sh
wasmop check install
```

---

### wasmop check

A top-level wrapper that runs environment readiness checks by invoking `wasmop check install`.

```sh
wasmop check
```

---

### wasmop setup kind

Creates a local Kubernetes cluster using Kind with configuration from `./devel/kind-config.yaml`. It interactively prompts for a cluster name and offers to mount a host directory into the Kind control-plane node (saving the host path as `WASMOP_HOST_FOLDER`).

```sh
wasmop setup kind
```

- **Arguments:** None (prompts interactively for `cluster_name`, `host_folder`, and `container_folder`).

---

### wasmop setup crd

Generates the Custom Resource Definition (CRD) manifest for the WasmOperator CRD from the controller crate and applies it to the active Kubernetes cluster.

```sh
wasmop setup crd
```

---

### wasmop setup

Runs the full initial setup workflow: checks tool installations, optionally provisions a Kind cluster interactively, and registers the CRD in the cluster.

```sh
wasmop setup
```

---

### wasmop build parent [parent_image_name]

Compiles the parent controller binary using Cargo and packages it into a Docker image.

```sh
wasmop build parent [parent_image_name]
```

- `[parent_image_name]`: _(Optional)_ The repository and tag name for the parent controller Docker image, defaulting to `wasmoperator-controller:latest`.

---

### wasmop load parent [kind_cluster_name][parent_image_name]

Builds the parent controller image, loads it into the specified Kind cluster, builds and loads the prediction webserver image, applies necessary RBAC and volume manifests, and restarts the parent controller pod.

```sh
wasmop load parent [kind_cluster_name] [parent_image_name]
```

- `[kind_cluster_name]`: _(Optional)_ The target Kind cluster name, defaulting to `wasm-operator`.
- `[parent_image_name]`: _(Optional)_ The parent controller Docker image tag to build and load, defaulting to `wasmoperator-controller:latest`.

This command requires a valid Kind cluster!

### wasmop build child <operator_name>

Compiles a child operator to WebAssembly targeting `wasm32-wasip2` from the current working directory, then copies the resulting `.wasm` binary into the mounted Kind host folder (`WASMOP_HOST_FOLDER`).

```sh
wasmop build child <operator_name>
```

- `<operator_name>`: _(Required)_ The binary name of the child operator without the `.wasm` extension (must match the Rust crate name with hyphens replaced by underscores).

This command requires a set `WASMOP_HOST_FOLDER` environment variable pointing to a valid host folder!

---

### wasmop load child <operator_name>

Builds the child operator WebAssembly module via `wasmop build child` and deploys it by deleting any existing instance and applying `./wasm_operator.yaml`.

```sh
wasmop load child <operator_name>
```

- `<operator_name>`: _(Required)_ The binary name of the child operator to build and deploy.

This command requires a set `WASMOP_HOST_FOLDER` environment variable pointing to a valid host folder!

---

### wasmop build [parent_image_name][operator_name]

Convenience command that sequentially builds both the parent controller Docker container and the child WebAssembly operator module.

```sh
wasmop build [parent_image_name] [operator_name]
```

- `[parent_image_name]`: _(Optional)_ Image tag for the parent controller, defaulting to `wasmoperator-controller:latest`.
- `[operator_name]`: _(Required for child build)_ The target name of the child operator.

This command requires a set `WASMOP_HOST_FOLDER` environment variable pointing to a valid host folder!

---

### wasmop load [kind_cluster_name][operator_name]

Convenience command that loads both the parent controller into the Kind cluster and deploys the child operator manifest.

```sh
wasmop load [kind_cluster_name] [operator_name]
```

- `[kind_cluster_name]`: _(Optional)_ Target Kind cluster name, defaulting to `wasm-operator`.
- `[operator_name]`: _(Required for child build)_ The target name of the child operator to build and load.

This command requires a valid Kind cluster!

This command requires a set `WASMOP_HOST_FOLDER` environment variable pointing to a valid host folder!
