#!/usr/bin/env bash

SOURCE_ROOT=$(realpath $(dirname "${BASH_SOURCE}"))
ROOT=$(realpath "${SOURCE_ROOT}/..")
CMD_ROOT=$(pwd)

source "${ROOT}/devel/tool.sh"

PKG_FOLDER="${ROOT}/pkg/controller"

ARCH=$(uname -m)

executable_exist() {
  local cmd="$1"
  if command -v "$cmd" &>/dev/null; then
    return 0 # executable was found
  fi
  return 1 # executable was not found
}

wasmop() (

    set -o errexit
    set -o pipefail

    local cmd="$1"
    shift
    if ! executable_exist "wasmop_${cmd}"; then
        echo "ERROR: tool '${cmd}' not found for wasm-operator"
        exit 1
    fi
    "wasmop_${cmd}" "$@"
)

wasmop_test() {
    echo "This works!"
}

wasmop_build() {
    local operator_name=$1

    export RUST_BACKTRACE=1
    export COMPILE_WITH_UNINSTANTIATE="TRUE"

    mkdir -p "./build"

    echo -e "\033[1m\nBuilding the parent operator\033[0m"
    cd "${PKG_FOLDER}"
    cargo build --release --target=${ARCH}-unknown-linux-musl --target-dir "${CMD_ROOT}/build/parent-target"
    
    cd "${CMD_ROOT}"
    echo -e "\033[1m\nBuilding the child operator\033[0m"
    #cargo component build --release --target wasm32-wasip2 --target-dir "./build/child-target"
    cargo build --release --target wasm32-wasip2 --target-dir "./build/child-target"

    cp ./build/parent-target/${ARCH}-unknown-linux-musl/release/controller ./build/parent_controller.bin
    cp ./build/child-target/wasm32-wasip2/release/${operator_name}.wasm ./build/${operator_name}.wasm  
}

wasmop_load() {
    local namespace=$1
    local image_name=$2
    local kind_cluster_name="kind"
    local get_flask_server=1
    # TODO: make that if dockerfile present take that one

    if [ -z "$namespace" ] || [ -z "$image_name" ]; then
        echo "ERROR: namespace and image_name parameters are required"
        exit 1
    fi

    echo -e "\033[1m\nBuilding and loading docker image for wasm-operator (parent + child controllers)\033[0m"

    mkdir -p ./build/docker-context

    # Copy built files to docker build context
    find ./build -maxdepth 1 -type f -exec cp {} ./build/docker-context/ \;
    cp ./wasm_config.yaml ./build/docker-context/wasm_config.yaml

    docker build ./build/docker-context -t $image_name -f "${PKG_FOLDER}/Dockerfile"
    kind load docker-image $image_name --name "${kind_cluster_name}"

    echo -e "\033[1m\nCreating kubernetes resources to run the wasm-operator\033[0m"
    # TODO: Maybe move these to the pkg folder?
    kubectl apply -f "${ROOT}/tests/yaml/metricsServer.yaml"
    kubectl apply -f "${ROOT}/tests/yaml/crd.yaml"
    kubectl apply -f "${ROOT}/tests/yaml/namespace.yaml"
    kubectl apply -f "${ROOT}/tests/yaml/rbac.yaml"

    echo -e "\033[34mCreate namespace and controller resource\033[0m"
    kubectl apply -f ./namespaces.yaml

    # Replace prediction server URL in child_controller.yaml
    if [ "$get_flask_server" -eq 1 ]; then
        echo -e "\033[34mGetting URL of prediction server\033[0m"
        SERVER="http://"
        SERVER+=$(kubectl get service/flask-service -o jsonpath='{.spec.clusterIP}')
        SERVER+=":5000/"
        echo "Server URL: $SERVER"
        sed "s|{{REPLACE.PREDICTION_SERVER_URL}}|$SERVER|" "./child_controller.yaml" > ./build/child_controller_parsed.yaml
        kubectl apply -f ./build/child_controller_parsed.yaml
    else
        kubectl apply -f ./child_controller.yaml
    fi

    # Wait for the controller pod to be running
    echo -e "\033[1m\nWaiting for the controller pod to be running\033[0m"
    #kubectl wait --namespace $namespace --for=condition=Ready pod/controller --timeout=3000s
    kubectl wait --namespace $namespace \
        --for=condition=Ready pods --all \
        --field-selector=status.phase!=Succeeded,status.phase!=Failed \
        --timeout=3000s

}
