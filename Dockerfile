# Multi-stage build for Aveloxis.
# Stage 1: Build the Go binary.
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

# go.mod requires go 1.26+ and the image carries it. The official images
# set GOTOOLCHAIN=local (no downloads); auto lets the build fetch a newer
# release when go.mod's toolchain line asks for one the image predates.
# scripts/go_minimum_version_test.go holds this comment and the image tag
# to go.mod.
ENV GOTOOLCHAIN=auto

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /aveloxis ./cmd/aveloxis

# Stage 2: Minimal runtime image.
FROM alpine:3.20

RUN apk add --no-cache git ca-certificates curl

COPY --from=builder /aveloxis /usr/local/bin/aveloxis

# Default config location.
WORKDIR /app
VOLUME ["/app", "/data"]

EXPOSE 5555 8082 8383

ENTRYPOINT ["aveloxis"]
CMD ["serve", "--workers", "4", "--monitor", ":5555"]
