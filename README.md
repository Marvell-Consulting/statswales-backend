# StatsWales Backend Service

This service is currently in beta and under active development, some features maybe incomplete, not working or missing.

## Requirements

-   Node 20+
-   PostgreSQL 16+
-   Azure Datalake
-   Docker

### Special requirements for windows

- Git bash

## Configuration

Copy the [.env-example](.env-example) file to `.env` and provide the missing values. The default dev setup uses Postgres
running in a Docker container for the datastore. Before running the service for the first time, the database schema must
be created and the initial data fixtures for the service need to be loaded.

```bash
# install dependencies
npm install

# start the database container
docker compose up -d db-dev

# run the migration(s)
npm run migration:run

# seed the db
npm run seed:required
```

## Running the service

Once the database is populated, you can start the app:

```bash
npm run dev
```

This will start the DB container in the background and run the app. The service should then be available on port 3001
by default (or whatever you specified for `BACKEND_PORT`).

### Special notes for windows

On occasion the redis connection will timeout on start up.  When this happens kill the instance of the backend and
try again.  This behaviour has not been seen on Mac or Linux systems.

## Testing the service

You can run the code checks and tests individually:

```bash
npm run prettier:fix
npm run lint:fix
npm run test
```

or all of them with one command:

```bash
npm run check
```

You can run the checks and then start the service with:

```bash
npm run dev:check
```

### Seeds for the frontend e2e tests

There are a number of fixtures (e.g. test users and sample datasets) used by the frontend e2e tests stored in
`/test/fixtures`. These must be loaded into the dev or test database before the e2e tests are run:

```bash
npm run seed:e2e
```

## Data migrations

The database schema is managed with [TypeORM](https://typeorm.io/) migrations.

Display the existing migrations:

```bash
npm run migration:show
```

Run any unexecuted migrations:

```bash
npm run migration:run
```

Rollback the most recent migration:

```bash
npm run migration:revert
```

Generate a new migration file after updating the entities (this does not execute the migration):

```bash
npm run migration:generate -- ./src/migration/<name>
```

e.g.
```bash
npm run migration:generate -- ./src/migration/initial-schema
```

## Deploying the service

The app is deployed as a container, based on [Dockerfile](Dockerfile).

## Service healthcheck

There a several routes for checking service availability and container health. A successful healthcheck will return a
200 response with the following body:
```
{ message: 'success' }
```

An endpoint to report if the service has started up:
```
GET /heathcheck
```

An endpoint that reports if the service is ready to receive requests. It checks for both a database connection and a
file store connection:
```
GET /healthcheck/ready
```

An endpoint that reports if the service is still in a healthy state. This is currently an alias for the
`/healthcheck/ready` route above.
```
GET /healthcheck/live
```
