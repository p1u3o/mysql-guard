FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
ARG VERSION=drill
ARG TAGS=
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -tags "${TAGS}" -ldflags="-s -w -X main.version=${VERSION}" -o /mysql-guard ./cmd/mysql-guard
RUN CGO_ENABLED=0 go build -trimpath -o /slowproxy ./integration/slowproxy

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /mysql-guard /usr/local/bin/mysql-guard
COPY --from=build /slowproxy /usr/local/bin/slowproxy
ENTRYPOINT ["mysql-guard"]
