#!/usr/bin/env bash

generate_wasm_config_yaml() {
  local name=$1
  local heap_mem_size=$2

  cat <<EOF
name: "$name"
wasm: ./$name.wasm
env:
- name: RUST_LOG
  value: "info"
- name: HEAP_MEM_SIZE
  value: "$heap_mem_size"
---
EOF
}

generate_controller_yaml() {
  local name=$1
  local namespace=$2
  local image=$3
  local server_address=$4

  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: ${namespace}
spec:
  serviceAccountName: custom-controller
  containers:
  - name: controller
    image: "${image}"
    imagePullPolicy: IfNotPresent
    env:
    - name: RUST_LOG
      value: "info"
    - name: PREDICTION_SERVER
      value: "${server_address}"
    resources:
      requests:
        memory: "640Mi"
      limits:
        memory: "1280Mi"
  
EOF

}

SCRIPT_ROOT=$(realpath $(dirname "${BASH_SOURCE}"))
source "${SCRIPT_ROOT}/tool.sh"
source "${SCRIPT_ROOT}/lib_functions.sh"

echo -e "\033[1mChecking required tools...\033[0m"
check_tool rust
check_tool docker
check_tool kind
check_tool kubectl
check_tool cross

cd "${SCRIPT_ROOT}/.."

ARCH=$(uname -m)

export RUST_BACKTRACE=1
export COMPILE_WITH_UNINSTANTIATE="TRUE"


# Parse command line arguments
CLEAN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Set variables
NAMESPACE="wasm-rust-simple" # Same as in namespace.yaml
NAME_OPERATOR="simple-child-controller" # Same as name used in cargo.toml of the child controller
BUILD_FOLER="./tests/temp_build_simple_rust"
HEAP_MEM_SIZE="90000000"
KIND_CLUSTER_NAME="kind"
IMAGE_NAME="wasm-operator-simple-rust:latest"

# Clean up previous build if requested
if [ "$CLEAN" = true ]; then
    echo -e "\033[1m\nCleaning up previous build...\033[0m"
    echo -e "\033[34mRemoving build folders\033[0m"
    run rm -rf "$BUILD_FOLER" pkg/controller/target controllers/simple-rust-controller/target
    echo -e "\033[34mRemoving kubernetes resources\033[0m"
    run kubectl delete TestResource --all --ignore-not-found=true
    run kubectl delete namespace wasm-rust-simple --ignore-not-found=true
    run kubectl delete pod/controller -n wasm-rust-simple --ignore-not-found=true
fi

## Build the WASM parent controller
echo -e "\033[1m\nCompile the parent controller\033[0m"
echo -e "\033[34mTeleporting to: $(pwd)/pkg/controller\033[0m"
pushd pkg/controller
run cargo build --release --target=${ARCH}-unknown-linux-musl
popd


## Build the WASM child controller
echo -e "\033[1m\nCompile the child controller defined via main.rs\033[0m"
echo -e "\033[34mTeleporting to: $(pwd)/controllers/simple-rust-controller\033[0m"
pushd controllers/simple-rust-controller
run cargo component build --release --features client-wasi --target-dir ./target
popd

echo -e "\033[34mTeleporting back to: $(pwd)\033[0m"

## Copy all the needed files into a single folder for loading into kind
# Make build folder
echo -e "\033[1m\nCopying all files to a temp folder to perpare building and loading\033[0m"
run mkdir -p "$BUILD_FOLER" 

# Copy parent controller
run cp pkg/controller/target/${ARCH}-unknown-linux-musl/release/controller "$BUILD_FOLER/parent_controller"
# Copy child controller wasm
run cp controllers/simple-rust-controller/target/wasm32-wasip1/release/${NAME_OPERATOR}.wasm "$BUILD_FOLER"

## Create a wasm_config.yaml file
echo -e "\033[1m\nCreating wasm_config.yaml file\033[0m"
generate_wasm_config_yaml $NAME_OPERATOR $HEAP_MEM_SIZE > "$BUILD_FOLER/wasm_config.yaml"
echo -e "\033[1;34mwasm_config.yaml content:\033[0m"
cat "$BUILD_FOLER/wasm_config.yaml"

## Build and load docker image for wasm-operator (parent + child controllers)
echo -e "\033[1m\nBuilding and loading docker image for wasm-operator (parent + child controllers)\033[0m"
run docker build -t $IMAGE_NAME -f ./tests/wasm_rust_simple/Dockerfile "$BUILD_FOLER"
run kind load docker-image $IMAGE_NAME --name "${KIND_CLUSTER_NAME}"

## Create kubernetes resources to run the wasm-operator
echo -e "\033[1m\nCreating kubernetes resources to run the wasm-operator\033[0m"
run kubectl apply -f ./tests/yaml/metricsServer.yaml
run kubectl apply -f ./tests/yaml/crd.yaml
run kubectl apply -f ./tests/yaml/namespace.yaml
run kubectl apply -f ./tests/yaml/rbac.yaml


## Create namespace and deploy the wasm-operator via namespace.yaml and pod.yaml resource definitions
echo -e "\033[1m\nDeploying the wasm-operator in the cluster\033[0m"
# Get URL of prediction server
echo -e "\033[34mGetting URL of prediction server\033[0m"
SERVER="http://"
SERVER+=$(kubectl get service/flask-service -o jsonpath='{.spec.clusterIP}')
SERVER+=":5000/"
echo "Server URL: $SERVER"

echo -e "\033[34mCreate namespace and controller resource\033[0m"
run kubectl apply -f ./tests/wasm_rust_simple/manifests/namespace.yaml
generate_controller_yaml "controller" "$NAMESPACE" $IMAGE_NAME $SERVER > "$BUILD_FOLER/pod.yaml"
run kubectl apply -f "$BUILD_FOLER/pod.yaml"


## Wait for the controller pod to be running
echo -e "\033[1m\nWaiting for the controller pod to be running\033[0m"
run kubectl wait --namespace $NAMESPACE --for=condition=Ready pod/controller --timeout=3000s