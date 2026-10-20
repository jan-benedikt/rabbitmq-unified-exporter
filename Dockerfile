# Multi-stage build for rabbitmq-unified-exporter (base images pinned by digest).
# TARGETOS/TARGETARCH are injected by docker buildx for multi-arch images.
FROM golang:1.26-alpine@sha256:397ecc648150dd585f1734dfcd8d1bb4746664e54464191d77ad0ecffaf38bdb AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=1.0.0
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/rabbitmq-unified-exporter \
    ./cmd/rabbitmq-unified-exporter

FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
COPY --from=builder /out/rabbitmq-unified-exporter /rabbitmq-unified-exporter
USER nonroot:nonroot
EXPOSE 9419
ENTRYPOINT ["/rabbitmq-unified-exporter"]
CMD ["-config", "/etc/rabbitmq-unified-exporter/config.yaml"]
