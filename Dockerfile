# Multi-stage build for Aveloxis.
# Stage 1: Build the Go binary.
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

# go.mod requires go 1.25+ but the latest Docker image is 1.24.
# GOTOOLCHAIN=auto tells Go to download the required toolchain version.
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

EXPOSE 5555 8080 8383

ENTRYPOINT ["aveloxis"]
CMD ["serve", "--workers", "4", "--monitor", ":5555"]
