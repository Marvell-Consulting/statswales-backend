FROM node:22-alpine

# Install build tools and distutils for node-gyp compatibility
RUN apk add --no-cache python3 py3-setuptools make g++ curl

# Create a non-root user and group
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . ./
RUN npm run build

RUN chown -R appuser:appgroup /app

HEALTHCHECK --interval=5m --timeout=3s \
    CMD curl --fail http://localhost:3000 || exit 1

ENV NODE_ENV=production
EXPOSE 3000

USER appuser

CMD ["sh", "-c", "npm run migration:run && exec node dist/server.js"]