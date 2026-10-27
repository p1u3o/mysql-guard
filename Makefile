VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
BINARY := mysql-guard
ARCHES := amd64 arm64

.PHONY: build test vet release clean

build:
	go build -ldflags="-X main.version=$(VERSION)" -o bin/$(BINARY) ./cmd/mysql-guard

test:
	go test ./...

vet:
	go vet ./...

release:
	mkdir -p dist
	set -e; for arch in $(ARCHES); do \
	  d=dist/$(BINARY)-$(VERSION)-linux-$$arch; rm -rf $$d; mkdir -p $$d; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o $$d/$(BINARY) ./cmd/mysql-guard; \
	  tar -C $$d -czf $$d.tar.gz $(BINARY); rm -rf $$d; \
	done
	cd dist && for f in $(BINARY)-$(VERSION)-linux-*.tar.gz; do sha256sum $$f > $$f.sha256; done && cat $(BINARY)-$(VERSION)-linux-*.tar.gz.sha256 > SHA256SUMS && cat SHA256SUMS

clean:
	rm -rf bin dist
