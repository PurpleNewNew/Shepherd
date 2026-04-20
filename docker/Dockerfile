FROM golang:1.22-bookworm AS build

WORKDIR /src

# Pre-fetch deps first for better layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build linux binaries for the target platform.
ARG TARGETOS=linux
ARG TARGETARCH
ENV CGO_ENABLED=0

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/kelpie ./cmd/kelpie
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/flock ./cmd/flock
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/shepherd-lab-bootstrap ./cmd/lab/bootstrap

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /out/kelpie /usr/local/bin/kelpie
COPY --from=build /out/flock /usr/local/bin/flock
COPY --from=build /out/shepherd-lab-bootstrap /usr/local/bin/shepherd-lab-bootstrap
