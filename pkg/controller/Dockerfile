# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/cc:debug-nonroot
WORKDIR /

COPY ./parent_controller.bin .

#COPY ./*.wasm ./
COPY ./wasm_config.yaml .
ENV  RUST_BACKTRACE=1
USER 65532:65532

#ENTRYPOINT ["/parent_controller.bin", "--debug", "/wasm_config.yaml"]
ENTRYPOINT ["/parent_controller.bin", "/wasm_config.yaml"]