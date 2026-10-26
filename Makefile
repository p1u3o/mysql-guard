VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
BINARY := mysql-guard
DIST := dist/$(BINARY)-$(VERSION)-linux-amd64.tar.gz

.PHONY: build test vet release clean

build:
	go build -o bin/$(BINARY) ./cmd/mysql-guard

test:
	go test ./...

vet:
	go vet ./...

release: $(DIST)

$(DIST):
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/$(BINARY) ./cmd/mysql-guard
	tar -C dist -czf $(DIST) $(BINARY)
	cd dist && sha256sum $$(basename $(DIST)) > $$(basename $(DIST)).sha256

clean:
	rm -rf bin dist
