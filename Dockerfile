# This is a multi-stage Dockerfile for the StatsWales backend application.

# This is the initial build image
# It installs the dependencies and transpiles the TypeScript code to JavaScript.
FROM node:24-slim AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . ./
RUN npm run build

# This is the deployable image
FROM node:24-slim AS runner

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

# install only production dependencies, then remove npm — it isn't needed at
# runtime (CMD runs node directly), and its bundled node-tar is what Trivy
# flags for CVE-2026-59873. Deleting it drops the vulnerable copy from the image.
RUN npm ci --omit=dev --no-audit --no-fund && \
    npm cache clean --force && rm -rf /root/.npm && \
    rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx

# copy in the built application source from the builder image
COPY --from=builder --chown=node:node /app/dist ./dist

HEALTHCHECK --interval=60s --timeout=3s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:3000/healthcheck/ || exit 1

ENV NODE_ENV=production
EXPOSE 3000

# Bake the git SHA into the image so the running app can report which commit it was built from.
# Passed at build time by CI (build-args: GIT_SHA=${{ github.sha }}); defaults to "unknown" for local builds.
ARG GIT_SHA=unknown
ENV GIT_SHA=${GIT_SHA}

# set the user to non-root (node)
USER node

CMD ["sh", "-c", "/app/node_modules/.bin/typeorm migration:run --dataSource=./dist/db/publisher-source.js && exec node dist/server.js"]
