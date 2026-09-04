# Kubernetes Operators in WebAssembly

This repository contains the code of a prototype runtime for running Kubernetes operators in WebAssembly. The goal is to improve the memory usage of a Kubernetes cluster by reducing the memory footprint of operators. This prototype reduces the overhead in three ways.

- It runs operators in a shared WebAssembly runtime to reduce the overhead of containerization.
- It swaps operators to disk when there are no changes to process.
- It uses the Rust programming language instead of Go.

For more information, read the paper [Adapting Kubernetes controllers to the edge: on-demand control planes using Wasm and WASI](https://doi.org/10.48550/arXiv.2209.01077) accepted to CloudNet 2022.

This project builds upon [this proof of concept](https://github.com/slinkydeveloper/extending-kubernetes-api-in-process-poc).

```text
+-- 📂examples                          # All child operators / wasmoperators that can be used as reference
|   +-- 📂ring-rust-operator            # Rust operator used in a ring benchmark
|   +-- 📂simple-rust-operator          # Rust operator used for simple tests and demonstrations

|   :
+-- 📂pkg                               # Packages developed for the WasmOperator Framework
|   +-- 📂controller                    # Parent controller
|   +-- 📂kube-rs                       # Modified kube-rs library used in WasmOperators to communicate with the parent controller
|   +-- 📂wasmtime                      # Modified Wasmtime library that enables snapshotting of the WebAssembly Store of Wasm Components
|   +-- 📂wit                           # WIT Interface for the communication between a WasmOperator and parent controller

|   :
+-- 📂devel                             # Tools for building & deploying

|   :
+-- 📂test                              # Deployment files for parent and metrics sever, and directory used as wasm source
+-- 📂prediction                        # Prediction related benchmarks/server
    +-- 📂webserver                     # Webserver flask api that predicts future values, used to predict wakeup times
:
```

## Get involved

This is an open source project, currently in the prototyping phase.
We greatly value feedback, bug reports, contributions,... during this stage of the project.

- to provide bug reports, feedback or suggestions, [create an issue](https://github.com/idlab-discover/wasm-operator/issues/new)
- to contribute code, see [contributing.md](docs/contributing.md)

## Getting started

### Setup of the project

A list of dependencies and the steps for getting started with the project can be found in the [Quick Start documentation](./docs/quick_start.md).

## Copyright

This code is released under the Apache License Version 2.0.

This prototype was initially developed by Tim Ramlot as part of his Master's dissertation.
This prototype was later extended by Kevin Van Landuyt as part of his Master's dissertation .

This work has been partially supported by the ELASTIC project, which received funding from the Smart Networks and Services Joint Undertaking (SNS JU) under the European Union’s Horizon Europe research and innovation programme under Grant Agreement No 101139067. Views and opinions expressed are however those of the author(s) only and do not necessarily reflect those of the European Union. Neither the European Union nor the granting authority can be held responsible for them.
