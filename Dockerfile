FROM node:22-slim

# Install build tools and ICU libs (adjust as needed for your app)
RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    build-essential \
    python3 \
    curl \
    libicu-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN groupadd --system appgroup && useradd --system --gid appgroup --create-home appuser

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . ./
RUN npm run build

RUN chown -R appuser:appgroup /app

HEALTHCHECK --interval=5m --timeout=3s \
    CMD curl --fail http://localhost:3000 || exit 1

ENV NODE_ENV=production
EXPOSE 3000

USER appuser

CMD ["sh", "-c", "npm run migration:run && exec node dist/server.js"]
