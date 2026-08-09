#!/usr/bin/env bash

echo -e "\033[1mWASM-OPERATOR DEVELOPMENT CLI LOADED\033[0m"

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
    ARCH="x86_64"
elif [[ "$ARCH" == armv* ]] || [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    ARCH="aarch64"
else
    echo "Unknown architecture: $ARCH. Defaulting to x86_64."
    ARCH="x86_64"
fi
OS=$(uname -s)

DEFAULT_CLUSTER_NAME="wasm-operator"
DEFAULT_PARENT_IMAGE_NAME="wasmoperator-controller:latest"

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
    fi

    CMD_ROOT=$(pwd)

    local cmd="$1"
    local sub_cmd="$2"
    local sub_sub_cmd="$3"

    if executable_exist "wasmop_${cmd}_${sub_cmd}_${sub_sub_cmd}"; then
        shift 3
        "wasmop_${cmd}_${sub_cmd}_${sub_sub_cmd}" "$@"
        exit 0
    elif executable_exist "wasmop_${cmd}_${sub_cmd}"; then
        shift 2
        "wasmop_${cmd}_${sub_cmd}" "$@"
        exit 0
    elif executable_exist "wasmop_${cmd}"; then
        shift 1
        "wasmop_${cmd}" "$@"
    else
        echo "ERROR: tool '${cmd}' not found for wasm-operator"
        exit 1
    fi
)

wasmop_test() {
    echo "This works!"
}

wasmop_config_list() {
    if [ -f "$CONFIG_FILE" ]; then
        echo "Current configuration variables in $CONFIG_FILE:"
        grep -E '^[A-Z_]+=' "$CONFIG_FILE" | while read -r line; do
            var_name=$(echo "$line" | cut -d= -f1)
            var_value=$(echo "$line" | cut -d= -f2- | tr -d '"')
            echo -e "\t$var_name: \033[32m$var_value\033[0m"
        done
    else
        echo "No configuration file found at $CONFIG_FILE."
    fi
}

wasmop_config_savevar() {
    local var_name="$1"
    local var_value="$2"

    if [ -z "$var_name" ] || [ -z "$var_value" ]; then
        echo "ERROR: Variable name and value are required"
        return 1
    fi

    if [ ! -f "$CONFIG_FILE" ]; then
        touch "$CONFIG_FILE"
    fi

    if grep -q "${var_name}" "$CONFIG_FILE"; then
        perl -i -pe "s|^\s*${var_name}=.*|${var_name}=\"${var_value}\"|" "$CONFIG_FILE"
    else
        printf "%s=\"%s\"\n" "$var_name" "$var_value" >> "$CONFIG_FILE"
    fi
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
        echo -e "No cluster name provided. Using default name \033[1m'wasm-operator'\033[0m."
        cluster_name=$DEFAULT_CLUSTER_NAME
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
            wasmop config savevar "WASMOP_HOST_FOLDER" "$host_folder"
        fi
    fi

    kind create cluster --name "${cluster_name}" --config "${config_file}"
}

# wasmop_setup_predictionserver() {
#     local cname=$1
#     if [ -n "$cname" ]; then
#         cluster_name="$cname"
#     else
#         if [ -z "$cluster_name" ]; then
#             echo "No cluster name provided."
#             exit 1
#         fi
#     fi

#     echo -e "\033[1m\nSetting up the prediction server in the cluster\033[0m"
#     docker build -t prediction_webserver:webserver "${ROOT}/prediction/webserver"
#     kind load docker-image --name $cluster_name prediction_webserver:webserver
#     kubectl apply -f "${ROOT}/tests/yaml/deploymentFlask.yaml"
# }

wasmop_setup_crd() {
    echo -e "\033[1m\nSetting up the CRD for the wasm-operator\033[0m"
    cd "${ROOT}/pkg/controller"

    temp_crd_manifest=$(mktemp)
    trap 'rm -f "$temp_crd_manifest"' EXIT

    cargo run --bin export_crd > "$temp_crd_manifest"
    kubectl apply -f "$temp_crd_manifest"
}

wasmop_setup() {
    wasmop check install

    if confirm "\nDo you want to create a kind cluster?"; then
        wasmop setup kind
    fi

    wasmop setup crd
}

# This will create a docker image for the parent controller that can be used in the Kubernetes cluster.
wasmop_build_parent() {
    local parent_image_name=$1
    if [ -z "$parent_image_name" ]; then
        parent_image_name=$DEFAULT_PARENT_IMAGE_NAME
    fi

    echo -e "\033[1m\nBuilding the parent controller image \033[32m'$parent_image_name'\033[0m"

    parent_target="${ARCH}-unknown-linux-musl"
    echo ">> Building the parent controller for target '${parent_target}'"
    cd "${PKG_FOLDER}"
    if [ "$OS" = "Darwin" ]; then
        # Use zigbuild for macOS to build for Linux
        cargo zigbuild --release --target=${parent_target}
    else
        cargo build --release --target=${parent_target}
    fi
    cp "./target/${parent_target}/release/controller" ./target/parent_controller.bin

    echo ">> Building and the docker image for the parent controller"
    docker build . -t ${parent_image_name}
}

# Build and load the parent controller image into the kind cluster
# This container/pod is then started with the parent_controller.yaml manifest provided in the tests/yaml folder.
wasmop_load_parent() {
    local kind_cluster_name=$1
    if [ -z "$kind_cluster_name" ]; then
        kind_cluster_name=$DEFAULT_CLUSTER_NAME
    fi

    local parent_image_name=$2
    if [ -z "$parent_image_name" ]; then
        parent_image_name=$DEFAULT_PARENT_IMAGE_NAME
    fi

    echo -e "\033[1m\nBuilding and loading the parent controller image into cluster \033[32m'$kind_cluster_name'\033[0m"
    wasmop build parent $parent_image_name
    kind load --name "${kind_cluster_name}" docker-image "${parent_image_name}"

    echo -e "\033[1m\nSetting up the prediction server in the cluster\033[0m"
    prediction_image_name="prediction-webserver:latest"
    docker build "${ROOT}/prediction/webserver" -t ${prediction_image_name}
    kind load --name "${kind_cluster_name}" docker-image "${prediction_image_name}"

    echo ">> Creating the right RBAC permissions for the parent controller"
    kubectl apply -f ${ROOT}/tests/yaml/parent_controller/rbac.yaml

    echo ">> Creating the right volumes for the parent controller"
    kubectl apply -f ${ROOT}/tests/yaml/parent_controller/volumes.yaml

    echo ">> Starting the parent controller in the cluster"
    kubectl delete -f ${ROOT}/tests/yaml/parent_controller/pod.yaml --ignore-not-found
    kubectl apply -f ${ROOT}/tests/yaml/parent_controller/pod.yaml
}

# Build the child controller wasm file and copy it to the host folder that is mounted into the kind cluster, so that the parent controller can load it from there.
wasmop_build_child() {
    local operator_name=$1
    if [ -z "$operator_name" ]; then
        echo -e "\033[1;31mERROR: No operator wasm file name provided. Use the same file name used in the WasmOperator CR and Rust package name (hyphens become underscores).\033[1;0m"
        return 1
    fi

    echo -e "\033[1m\n>> Building the child controller\033[0m"
    cd "${CMD_ROOT}"
    export RUSTFLAGS="--cfg tokio_unstable"
    cargo build --release --target wasm32-wasip2

    echo -e "\033[1m\n>> Copying the built wasm file to the host folder for the kind cluster\033[0m"
    cp "./target/wasm32-wasip2/release/${operator_name}.wasm" "${WASMOP_HOST_FOLDER}/${operator_name}.wasm"
}

# Build the child operator and start the operator by applying the wasm_operator.yaml
wasmop_load_child() {
    wasmop build child $1

    cd "${CMD_ROOT}"
    kubectl delete -f ./wasm_operator.yaml --ignore-not-found
    kubectl apply -f ./wasm_operator.yaml
}


wasmop_build() {
    wasmop build parent $1
    wasmop build child $2
}

wasmop_load() {
    wasmop load parent $1
    wasmop load child $2
}
