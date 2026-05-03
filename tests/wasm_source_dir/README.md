# Wasm source directory

## Prupose
You can use this directory to store your WebAssembly modules that represent child operators. This directory is mounted as a volume in the kind cluster to the path `/wasm_source_dir`. You can create a PersistentVolume and PersistentVolumeClaim that points to this directory, and then reference the PVC in your WasmOperator CRD to load the WebAssembly module from there.

This directory should be empty in Git except for this README.md file, as the WebAssembly modules are expected to be built locally and not stored in version control.