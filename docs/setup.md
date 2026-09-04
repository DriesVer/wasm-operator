# Setting Up the WASM-Operator in a Generic Kubernetes Cluster

This guide explains how to set up the WASM-Operator framework in a general Kubernetes cluster. This tutorial assumes that you have a working Kubernetes cluster and the necessary permissions to deploy resources. The steps below will guide you through the process of setting up the WASM-Operator framework, including building and deploying the parent controller and optionally the prediction server.

To get started, you can optionally load the Development CLI helper script:

```sh
source ./devel/cli.sh
```

## 1. Cloning the Repository

Clone the repository including all submodules:

```sh
git clone --recurse-submodules git@github.com:DriesVer/wasm-operator.git
```

If you already cloned it without submodules, initialize them:

```sh
git submodule update --init --recursive
```

## 2. Dependencies

Make sure the following tools are installed on your device:

- Kind
- Kubectl
- Docker
- Rust
- Cargo
- Cargo Zigbuild (required on macOS)

- **With CLI**: You can verify your environment by running:
  ```sh
  wasmop check
  ```

## 3. Install WasmOperator CRD

Apply the Custom Resource Definition (CRD) for the WasmOperator to your cluster. The WasmOperator CRD manifest can be generated using the Rust scrip `export_crd` in the `pkg/controller` directory or directly applied using the CLI.

- **With CLI**:
  ```sh
  wasmop setup crd
  ```
- **Manually**:
  ```sh
  cd pkg/controller
  cargo run --bin export_crd > crd.yaml
  kubectl apply -f crd.yaml
  ```

## 4. Build the Parent Controller Image

You must compile the parent controller and package it into a Docker image. You can change the default behaviour of the parent controller by setting environment variables during the build process and at runtime. See the [Parent Controller Environment Variables](./parent.md) documentation for more details. This can only be done when building the parent controller manually, not when using the CLI.

- **With CLI**:
  ```sh
  wasmop build parent <image_name>
  ```
- **Manually**:
  First, install the appropriate targets and dependencies (depending on your host OS):

  ```sh
  rustup target add x86_64-unknown-linux-musl   # For x86 architectures
  rustup target add aarch64-unknown-linux-musl  # For ARM architectures
  sudo apt install -y musl musl-dev musl-tools build-essential
  ```

  Then build the binary of the parent and docker image. Change architecure if needed. Be sure to choose the appropriate compile time environment variables such as enabling or disabling the prediction feature, changing the idle threshold, etc. See [Parent Controller Environment Variables](./parent.md) for more details.

  ```sh
  cd pkg
  cargo build --release --target x86_64-unknown-linux-musl
  cp ./target/x86_64-unknown-linux-musl/release/controller ./target/parent_controller.bin
  docker build . -t <image_name>
  ```

## 5. Build the Prediction Server Image (Optional)

The prediction server runs as a sidecar alongside the parent controller. If the prediction feature was disabled during the parent controller build, you can skip this step. Otherwise, you need to build the prediction server image.

- **Manually**:
  ```sh
  docker build ./prediction/webserver -t <prediction_image_name>
  ```

## 6. Make Images Available in the Cluster

Since you are using a generic Kubernetes cluster, the `wasmop load parent` command cannot be used to load images (as it relies on Kind).
Make sure the container images for the parent controller and the prediction server are available in the cluster via, for example, a container registry or by loading them into the cluster nodes directly.

## 7. Deploy to the Cluster

Set up the necessary RBAC, Volumes, and deploy the parent controller pod.

- **Manually**:
  Apply RBAC rules:
  ```sh
  kubectl apply -f tests/yaml/parent_controller/rbac.yaml
  ```
  This will create a ServiceAccount and bind the necessary permissions to it to allow the parent controller to watch for `WasmOperator` resources and manage them.

If you are using a hostPath volume to load WasmOperator source files, apply the volume definitions. This is not necessary if you are using another source type. (Currently no alternative source type is supported, but this may change in the future.)

```sh
kubectl apply -f tests/yaml/parent_controller/volumes.yaml
```

_(Note: You may need to modify the volume definitions to fit your cluster's storage configuration, such as using persistent volume claims instead of a hostPath)._

Update the image names in `tests/yaml/parent_controller/pod.yaml` to match the images you pushed to your registry, then apply it. Be sure to set the correct environment variables and use a prediction side if needed for your use case. See [Parent Controller Environment Variables](./parent.md) for more details.

```sh
kubectl apply -f tests/yaml/parent_controller/pod.yaml
```

Verify that the controller started cleanly:

```sh
kubectl logs -f controller
```
