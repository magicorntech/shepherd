# Cross-compiles on the build host's own architecture, so a multi-arch
# `docker buildx build --platform linux/amd64,linux/arm64` needs no QEMU.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /shepherd ./cmd/shepherd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /shepherd /shepherd
USER nonroot:nonroot
ENTRYPOINT ["/shepherd"]
