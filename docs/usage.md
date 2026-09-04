# Deploying a WasmOperator in a Generic Kubernetes Cluster

This guide explains how to deploy a child WasmOperator (using the `simple-rust-operator` example) to a general Kubernetes cluster.
This tutorial assumes that the parent controller is already running in the cluster. If this is not the case, please refer to the [Quick Start documentation](./quick_start.md) or the [Setup documentation](./setup.md) to set up the parent controller in your cluster.

## 0. Write a WasmOperator

First, you need to write a WasmOperator or convert an existing operator to a WasmOperator. The operators must implement the WIT interface defined in the [`pkg/wit`](../pkg/wit/) package. For a reference implementation, you can check the [`simple-rust-operator`](../examples/simple-rust-operator/) example in the `examples` directory.

Secondly, you need to create a `WasmOperator` custom resource that instructs the parent controller to load and manage the child operator. You can find an example of this in the [`wasm_operator.yaml`](../examples/simple-rust-operator/wasm_operator.yaml) file and a description of the WasmOperator CRD fields in the [WasmOperator CRD documentation](./wasm_operator_crd.md).

## 1. Apply Target CRD and RBAC

The child operator needs the Custom Resource Definition it will manage, as well as the necessary RBAC permissions for the parent controller to reconcile it.
The default service account of the parent controller can be found in the file [Parent controller RBAC](../tests/yaml/parent_controller/rbac.yaml).

Navigate to the example directory (or the directory of your own child operator):

```sh
cd examples/simple-rust-operator/
```

Apply the resources manually:

```sh
kubectl apply -f test-manifests/TestResource_CRD.yaml
kubectl apply -f test-manifests/rbac.yaml
```

## 2. Build the Child Operator

Compile the child operator into a WebAssembly module.

- **With CLI**:
  ```sh
  wasmop build child simple_rust_operator
  ```
- **Manually**:
  ```sh
  cargo build --release --target wasm32-wasip2
  ```

## 3. Place the WASM File in the configured source of the WasmOperator

The parent controller requires access to the compiled `.wasm` file to load the operator.
Make sure the compiled file (e.g., `target/wasm32-wasip2/release/simple_rust_operator.wasm`) is available in the source configured in the WasmOperator custom resource. This is typically a shared volume or a specific directory in the cluster.

The CLI automatically attempts to copy this file to the path specified in `WASMOP_HOST_FOLDER`, but for a generic Kubernetes cluster, you may need to upload it to a persistent volume or synchronize it with the cluster's storage manually.

## 4. Deploy the WasmOperator Resource

Create the `WasmOperator` custom resource to instruct the parent controller to load and manage the child operator.

- **With CLI**:
  The CLI can build and deploy in one step (assuming the shared volume path is properly configured):
  ```sh
  wasmop load child simple_rust_operator
  ```
- **Manually**:
  Once the `.wasm` file is in the shared volume, apply the manifest:
  ```sh
  kubectl apply -f ./wasm_operator.yaml
  ```

## 5. Verify the Deployment

Check the parent controller logs to confirm the module loaded successfully.

Test the reconciliation by creating a resource that the child operator watches:

```sh
kubectl apply -f test-manifests/function1.yaml
kubectl get testresource
```

The output should confirm the child operator successfully calculated the result.
