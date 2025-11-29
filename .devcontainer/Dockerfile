# Use Go 1.25.4 base
FROM golang:1.25.4-trixie AS dev

ENV CGO_ENABLED=1
ENV LANG=C.UTF-8

# Install Node + npm + pnpm support
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs

# Optional: install pnpm globally via corepack
RUN corepack enable

RUN apt-get update && apt-get install -y \
    libvips-dev \
    pkg-config \
    build-essential

WORKDIR /workspace
