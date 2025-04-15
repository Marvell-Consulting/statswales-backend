FROM node:22

WORKDIR /app

# Optimise build layering by installing dependencies before copying in the rest of the application. This way, if only
# the application code changes, we can use the cached layer for npm install to speed up the build.
COPY package*.json ./
RUN npm install
COPY . ./
RUN npm run build

HEALTHCHECK --interval=5m --timeout=3s \
    CMD curl --fail http://localhost:3000 || exit 1

ENV NODE_ENV=production
EXPOSE 3000

# Run any pending database migrations before starting the server
CMD /usr/local/bin/npm run migration:run ; exec /usr/local/bin/node dist/server.js
