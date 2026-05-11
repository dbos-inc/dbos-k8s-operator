# Lightweight runtime image. Cross-compilation happens on the host (see
# Makefile docker-build target) — Docker only packages the binary.
FROM gcr.io/distroless/static-debian12:nonroot
COPY bin/operator /usr/local/bin/operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/operator"]
