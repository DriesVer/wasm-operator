#!/usr/bin/env bash

if [ -n "$BASH_VERSION" ]; then
    SOURCE_ROOT=$(realpath $(dirname "${BASH_SOURCE}"))
elif [ -n "$ZSH_VERSION" ]; then
    SOURCE_ROOT=$(realpath $(dirname "${(%):-%x}"))
else
    echo "Unsupported shell. Please use bash or zsh."
    exit 1
fi

ROOT=$(realpath "${SOURCE_ROOT}/..")
PKG_FOLDER="${ROOT}/pkg/controller"

CONFIG_FILE="${SOURCE_ROOT}/wasmop_config.sh"

ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "amd64" ]; then
    ARCH="amd64"
elif [[ "$ARCH" == armv* ]] || [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    ARCH="aarch64"
else
    echo "Unknown architecture: $ARCH. Defaulting to amd64."
    ARCH="amd64"
fi
OS=$(uname -s)

executable_exist() {
  local cmd="$1"
  if command -v "$cmd" &>/dev/null; then
    return 0 # executable was found
  fi
  return 1 # executable was not found
}

confirm() {
    echo -n "$1 [Y/n]: "
    read response
    case "$response" in
        [yY][eE][sS]|[yY]|"") 
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

wasmop() (
    set -o errexit
    set -o pipefail

    if [ -f "$CONFIG_FILE" ]; then
        source "$CONFIG_FILE"
    else
        wasmop_createconfig
    fi

    CMD_ROOT=$(pwd)

    local cmd="$1"
    local sub_cmd="$2"

    shift
    if executable_exist "wasmop_${cmd}_${sub_cmd}"; then
        shift
        "wasmop_${cmd}_${sub_cmd}" "$@"
        exit 0
    elif executable_exist "wasmop_${cmd}"; then
        "wasmop_${cmd}" "$@"
    else
        echo "ERROR: tool '${cmd}' not found for wasm-operator"
        exit 1
    fi
)

wasmop_test() {
    echo "This works!"
}

wasmop_createconfig() {
    content="
    HOST_FOLDER=
    "
    printf "%s\n" "$content" > "$CONFIG_FILE"
}

wasmop_check_install() {
    echo "Checking for required tools:"
    if ! executable_exist "kind"; then
        echo -e "\033[31mERROR: kind is not installed. Please install kind to proceed.\033[0m"
        exit 1
    else 
        echo -e "\tkind: \033[32m[installed]\033[0m $(kind version)"
    fi

    if ! executable_exist "kubectl"; then
        echo -e "\033[31mERROR: kubectl is not installed. Please install kubectl to proceed.\033[0m"
        exit 1
    else 
        echo -e "\tkubectl: \033[32m[installed]\033[0m See kubectl version for details"
    fi

    if ! executable_exist "docker"; then
        echo -e "\033[31mERROR: docker is not installed. Please install docker to proceed.\033[0m"
        exit 1
    else 
        echo -e "\tdocker: \033[32m[installed]\033[0m $(docker --version)"
    fi

    if ! executable_exist "rustc"; then
        echo -e "\033[31mERROR: rustc is not installed. Please install rustc to proceed.\033[0m"
        exit 1
    else 
        echo -e "\trustc: \033[32m[installed]\033[0m $(rustc --version)"
    fi

    if ! executable_exist "cargo"; then
        echo -e "\033[31mERROR: cargo is not installed. Please install cargo to proceed.\033[0m"
        exit 1
    else 
        echo -e "\tcargo: \033[32m[installed]\033[0m $(cargo --version)"
    fi

    if [ "$OS" = "Darwin" ]; then
        if ! executable_exist "cargo-zigbuild"; then
            echo -e "\033[31mERROR: cargo-zigbuild is not installed. Please install cargo-zigbuild to proceed.\033[0m"
            exit 1
        else 
            echo -e "\tcargo-zigbuild: \033[32m[installed]\033[0m $(cargo-zigbuild --version)"
        fi
    fi

    echo "All required tools are installed."
}

wasmop_check() {
    echo -e "\033[1m\nChecking the environment for wasm-operator\n\033[0m"
    wasmop check install
}

wasmop_setup_kind() {
    echo -n "Kind cluster name: " 
    read cluster_name
    if [ -z "$cluster_name" ]; then
        echo "No cluster name provided. Using default name \033[1m'wasm-operator'\033[0m."
        cluster_name="wasm-operator"
    fi

    echo "Using kind config from './devel/kind-config.yaml'."
    config_file="${ROOT}/devel/kind-config.yaml"

    if [ ! -f "$config_file" ]; then
            echo -e "\033[31mERROR: Kind config file not found at '$config_file'. Please create the config file or provide the correct path.\033[0m"
            exit 1
        fi

    if confirm "Do you want to mount a folder into the kind cluster?"; then
        echo -n "Host folder to mount: " 
        read host_folder
        if [ -z "$host_folder" ]; then
            echo "No host folder provided. Skipping folder mount."
        else
            echo -n "Container folder to mount to (default: /mnt/host): " 
            read container_folder
            if [ -z "$container_folder" ]; then
                container_folder="/mnt/host"
            fi
            echo "Mounting host folder '$host_folder' to container folder '$container_folder' in kind cluster '$cluster_name'."

            temp_config=$(mktemp)
            trap 'rm -f "$temp_config"' EXIT
            cp "${ROOT}/devel/kind-config.yaml" "$temp_config"

            perl -i -pe "s|role: control-plane|role: control-plane\n  extraMounts:\n  - hostPath: ${host_folder}\n    containerPath: ${container_folder}|" "$temp_config"
            
            config_file="$temp_config"

            # Save the host folder path to the config file for later use
            perl -i -pe "s|^\s*HOST_FOLDER=.*|HOST_FOLDER=\"$host_folder\"|" "$CONFIG_FILE"
        fi
    fi

    kind create cluster --name "${cluster_name}" --config "${config_file}"
}

wasmop_setup_predictionserver() {
    local cname=1
    if [ -n "$cname" ]; then
        cluser_name="$cname"
    else
        if [ -z "$cluster_name" ]; then
            echo "No cluster name provided."
            exit 1
        fi
    fi

    echo -e "\033[1m\nSetting up the prediction server in the cluster\033[0m"
    docker build -t prediction_webserver:webserver "${ROOT}/prediction/webserver"
    kind load docker-image --name $cluster_name prediction_webserver:webserver
    kubectl apply -f "${ROOT}/tests/yaml/deploymentFlask.yaml"
}

wasmop_setup_crd() {
    echo -e "\033[1m\nSetting up the CRD for the wasm-operator\033[0m"
    cd "${ROOT}/pkg/controller"

    temp_crd_manifest=$(mktemp)
    trap 'rm -f "$temp_crd_manifest"' EXIT
    echo $temp_crd_manifest

    cargo run --bin export_crd > "$temp_crd_manifest"
    kubectl apply -f "$temp_crd_manifest"
}

wasmop_setup() {
    wasmop check install

    if confirm "\nDo you want to create a kind cluster?"; then
        wasmop setup kind
    fi

    if confirm "\nDo you want to install the prediction server in the cluster?"; then
        wamop setup predictionserver
    fi

    wasmop setup crd
}

wasmop_build() {
    local operator_name=$1

    export RUST_BACKTRACE=1
    export COMPILE_WITH_UNINSTANTIATE="TRUE"
    export RUSTFLAGS="-A warnings"

    mkdir -p "./build"

    echo -e "\033[1m\nBuilding the parent operator\033[0m"
    cd "${PKG_FOLDER}"
    parent_target="${ARCH}-unknown-linux-musl"
    if [ "$OS" = "Darwin" ]; then
        # Use zigbuild for macOS to build for Linux
        cargo zigbuild --release --target=${parent_target} --target-dir "${CMD_ROOT}/build/parent-target"
    else
        cargo build --release --target=${parent_target} --target-dir "${CMD_ROOT}/build/parent-target"
    fi
    
    cd "${CMD_ROOT}"
    echo -e "\033[1m\nBuilding the child operator\033[0m"
    #cargo component build --release --target wasm32-wasip2 --target-dir "./build/child-target"
    cargo build --release --target wasm32-wasip2 --target-dir "./build/child-target"

    cp ./build/parent-target/${parent_target}/release/controller ./build/parent_controller.bin
    cp ./build/child-target/wasm32-wasip2/release/${operator_name}.wasm ./build/${operator_name}.wasm  
    cp ./build/${operator_name}.wasm ${ROOT}/tests/wasm_source_dir/${operator_name}.wasm
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
        # kubectl apply -f ./build/child_controller_parsed.yaml
        kubectl replace --force -f ./build/child_controller_parsed.yaml
    else
        # kubectl apply -f ./child_controller.yaml
        kubectl replace --force -f ./child_controller.yaml
    fi

    # Wait for the controller pod to be running
    echo -e "\033[1m\nWaiting for the controller pod to be running\033[0m"
    kubectl wait --namespace $namespace \
        --for=condition=Ready pods --all \
        --field-selector=status.phase!=Succeeded,status.phase!=Failed \
        --timeout=3000s

}
