# StatsWales Backend Service

This service is currently in beta and under active development, some features maybe incomplete, not working or missing.

## Requirements

-   Node 20+
-   PostgreSQL 16+
-   Azure Datalake
-   Docker

## Configuration

Copy the [.env-example](.env-example) file to `.env` and provide the missing values. The default dev setup uses Postgres
running in a Docker container for the datastore. The data volume is persisted in `.docker/postgres/data`.

## Running the service

Once you've created the configuration, run:

```bash
npm install
npm run dev
```

This will start the DB container in the background and run the app. The service should then be available on port 3000
by default (or whatever you specified for `BACKEND_PORT`).

## Testing the service

You can run the checks individually (`prettier`, `lint`, `test`, `build`) or all of them with one command:

```bash
npm run check
```

You can run the checks and the service with:

```bash
npm run dev:check
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
npm run migration:generate -- ./src/migration/create_user_table
```

## Deploying the service

The app is deployed as a container, based on [Dockerfile](Dockerfile).
