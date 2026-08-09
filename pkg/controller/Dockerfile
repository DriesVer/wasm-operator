# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/cc:debug-nonroot
WORKDIR /

COPY ./target/parent_controller.bin .

ENV  RUST_BACKTRACE=1
USER 65532:65532

ENTRYPOINT ["/parent_controller.bin"]