# `make test` is exactly what CI runs. Nothing gets committed without it green.
HELM_VERSION ?= 3.21.4
# kube-apiserver + etcd for the integration tests (controller-runtime envtest).
ENVTEST_K8S_VERSION ?= 1.37.0
SETUP_ENVTEST       ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
BIN          := $(CURDIR)/.bin
export PATH  := $(BIN):$(PATH)

IMG     ?= public.ecr.aws/magicorn/shepherd
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

UNAME_S := $(shell uname -s | tr A-Z a-z)
UNAME_M := $(shell uname -m)
HELM_ARCH := $(if $(filter x86_64,$(UNAME_M)),amd64,$(if $(filter aarch64 arm64,$(UNAME_M)),arm64,$(UNAME_M)))

.PHONY: test go-test unit vet chart-test versions build image tools clean

test: vet go-test versions chart-test

tools: $(BIN)/helm
$(BIN)/helm:
	@mkdir -p $(BIN)
	curl -fsSL https://get.helm.sh/helm-v$(HELM_VERSION)-$(UNAME_S)-$(HELM_ARCH).tar.gz \
	  | tar -xz -C $(BIN) --strip-components=1 $(UNAME_S)-$(HELM_ARCH)/helm

vet:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "gofmt needed"; exit 1; }
	go vet ./...
# Unit + integration tests. The integration tests start a real kube-apiserver
# and etcd, downloaded on first use (~100 MB, cached under the user cache dir).
go-test:
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" go test -race -count=1 ./...

# Fast loop: unit tests only. Integration tests skip themselves when
# KUBEBUILDER_ASSETS is unset.
unit:
	go test -race ./...
versions:
	ci/check-versions.sh
chart-test: tools
	ci/chart-test.sh

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/shepherd ./cmd/shepherd
image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMG):$(VERSION) .

clean:
	rm -rf bin .bin
