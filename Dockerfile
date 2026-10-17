# ==============================================================================
# AuraKafka - Docker Container Specification
# Multi-stage image for Web Portal & Native Aura Toolchain
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Build Web Portal & Dashboard (React + TypeScript + Vite)
# ------------------------------------------------------------------------------
FROM node:20-slim AS web-builder

WORKDIR /app/web

COPY web/package*.json ./
RUN npm install

COPY web/ ./
RUN npm run build

# ------------------------------------------------------------------------------
# Stage 2: Web Portal Runtime (Lightweight Node.js Server & Dashboard)
# ------------------------------------------------------------------------------
FROM node:20-slim AS web

LABEL maintainer="Aura Core Team <dev@aura-lang.org>"
LABEL org.opencontainers.image.title="AuraKafka Web Portal & Observability Dashboard"
LABEL org.opencontainers.image.description="Embedded real-time dashboard and administration portal for AuraKafka"

WORKDIR /app

# Install dependencies in web directory
COPY web/package*.json ./web/
RUN cd web && npm install

# Copy application files and built static dist
COPY web/ ./web/
COPY --from=web-builder /app/web/dist ./web/dist

WORKDIR /app/web

EXPOSE 9090

ENV PORT=9090
ENV NODE_ENV=production

# Healthcheck validating the cluster REST API
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "const http = require('http'); http.get('http://127.0.0.1:9090/api/cluster', (res) => process.exit(res.statusCode === 200 ? 0 : 1)).on('error', () => process.exit(1));"

CMD ["npm", "run", "server"]

# ------------------------------------------------------------------------------
# Stage 3: Aura Compiler Builder (Optional for running AuraKafka CLI inside Docker)
# ------------------------------------------------------------------------------
FROM rust:slim-bookworm AS aura-builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/aura-lang-developer/aura-lang.git . && \
    mkdir -p playground && echo '<!DOCTYPE html><html><body><h1>Aura Playground</h1></body></html>' > playground/index.html && \
    cargo build --release --bin aurac -j 2

# ------------------------------------------------------------------------------
# Stage 4: AuraKafka CLI & Toolchain Runtime
# ------------------------------------------------------------------------------
FROM node:20-slim AS tools

LABEL maintainer="Aura Core Team <dev@aura-lang.org>"
LABEL org.opencontainers.image.title="AuraKafka CLI & Toolchain"
LABEL org.opencontainers.image.description="Complete Aura compiler toolchain and AuraKafka CLI tools"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    bash \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy aurac compiler binary from aura-builder stage
COPY --from=aura-builder /build/target/release/aurac /usr/local/bin/aurac
RUN ln -s /usr/local/bin/aurac /usr/local/bin/aura

# Copy repository sources
COPY . /app/
RUN chmod +x /app/start-local.sh && mkdir -p /app/bin /app/data

CMD ["/bin/bash", "-c", "./start-local.sh all"]
