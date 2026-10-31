# syntax=docker/dockerfile:1

# Match this to the "go" line in go.mod (newer toolchains are fetched automatically).
ARG GO_VERSION=1.24

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags "-s -w -X github.com/sqmi511/raftkv/internal/version.Version=${VERSION}" -o /out/raftkv-server ./cmd/server \
 && go build -trimpath -ldflags "-s -w -X github.com/sqmi511/raftkv/internal/version.Version=${VERSION}" -o /out/raftkv-cli ./cmd/cli

FROM alpine:3.20
RUN adduser -D -u 10001 raftkv && mkdir /data && chown raftkv /data
COPY --from=build /out/raftkv-server /out/raftkv-cli /usr/local/bin/
USER raftkv
VOLUME /data
# 7000: client + peer gRPC, 8000: health and metrics
EXPOSE 7000 8000
ENTRYPOINT ["raftkv-server"]
CMD ["-data-dir=/data"]