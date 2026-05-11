FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/operator ./cmd/operator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/operator /usr/local/bin/operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/operator"]
