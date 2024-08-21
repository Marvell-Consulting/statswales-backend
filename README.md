# StatsWales Backend Service

> This service is currently in beta and under active development
> some features maybe incomplete, not working or missing.

## Requirements

-   Node 20+
-   PostgreSQL 16+
-   Azure Datalake

## To get going

You'll need to define the following environment variables either in the environment or in a `.env` file:

```env
DB_HOST
DB_PORT
DB_USERNAME
DB_PASSWORD
DB_DATABASE
AZURE_STORAGE_ACCOUNT_NAME
AZURE_STORAGE_ACCOUNT_KEY
AZURE_STORAGE_DIRECTORY_NAME
AZURE_BLOB_STORAGE_ACCOUNT_NAME
AZURE_BLOB_STORAGE_ACCOUNT_KEY
AZURE_BLOB_STORAGE_CONTAINER_NAME
```

and optionally:

```env
BACKEND_PORT
DB_SSL
```

To run the app should be as simple as:

```bash
npm install
npm run dev
```

The app should then be available on port 3000 by default (or whatever you specified for `BACKEND_PORT`).
