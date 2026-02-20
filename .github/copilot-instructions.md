# StatsWales Backend — Copilot Instructions

## Architecture

REST API for a statistical data publishing platform. Handles dataset lifecycle: upload, revision management, dimension/measure configuration, cube building (DuckDB), publishing workflows, and public consumer APIs.

**Two Express apps share this codebase:**
- Publisher API — JWT-protected routes for dataset management (`/dataset`, `/build`, `/provider`, etc.)
- Consumer API — public read-only routes versioned at `/v1` and `/v2`

**Key src/ directories:**
- `controllers/` — exported async functions (not classes), one file per domain
- `routes/` — thin Express `Router` definitions wiring verbs to controller functions; `consumer/v1/` and `consumer/v2/` sub-routes for the public API
- `services/` — business logic; `DatasetService` is request-scoped and injected via `req.datasetService`
- `entities/` — TypeORM entities in `dataset/`, `user/`, `task/` subdirectories; UUID PKs, explicit snake_case column names and FK constraint names
- `repositories/` — plain exported functions (not class extensions); `FindOptionsRelations` presets named `withXxx` for explicit eager-loading
- `dtos/` — plain classes with `snake_case` public fields and `static fromXxx()` factory methods; no class-transformer
- `exceptions/` — typed error classes with a `status` code, each extending `Error` (e.g. `NotFoundException(404)`, `ForbiddenException(403)`)
- `migrations/` — TypeORM migrations; run with `npm run migration:run`
- `middleware/services.ts` — creates request-scoped `fileService` and `DatasetService`, attaches to `req`

## Build and Test

```bash
npm run dev          # starts docker deps + ts-node-dev with pino logging
npm run build        # tsc + copy-assets + generate-docs
npm run test         # jest --coverage (pretest starts db-test via docker-compose)
npm run test:ci      # jest --ci --coverage
npm run lint:fix     # eslint --fix
npm run migration:run
npm run seed:required
npm run init:ci      # migrate + seed required + seed tests (CI)
```

Tests are **integration-style** against a real Postgres test DB on port 5433, serialised (`maxWorkers: 1`). HTTP tests use `supertest`. Setup file: `test/helpers/jest-setup.ts`. Test helpers in `test/helpers/` provide auth headers and seed/teardown utilities.

Coverage thresholds are defined in `jest.config.ts` — **raise them when adding new tests** so the floor keeps pace with the codebase.

## Code Style

- Files: `kebab-case.ts`; classes: `PascalCase`; controller functions: `camelCase` async named exports
- Enums in `src/enums/` with `kebab-case` filenames, `PascalCase` enum names
- Controllers call `next(new SomeException())` — never throw directly to Express; the global `errorHandler` in `src/routes/error-handler.ts` handles all typed exceptions
- DTOs map entities via explicit `static fromXxx()` — do not add class-transformer decorators
- TypeORM entities use `@PrimaryGeneratedColumn('uuid')`, explicit `name:` on every column, and explicit `foreignKeyConstraintName` on every FK

## Key Patterns

**Repository presets:**
```ts
// src/repositories/dataset.ts
export const withDimensions: FindOptionsRelations<Dataset> = { dimensions: true, ... };
DatasetRepository.getById(id, withDimensions);
```

**Service injection:**
```ts
// src/middleware/services.ts attaches per request
req.datasetService = new DatasetService(dataSource, req.user);
```

**Async local storage** (`src/middleware/request-context.ts`) — use `asyncLocalStorage.getStore()` anywhere in the call stack for request-scoped data (audit, request IDs).

**Auth:**
- Primary: `passport-jwt` Bearer token; all protected routes use `passport.authenticate('jwt', { session: false })`
- Optional: EntraID (Azure AD) OIDC via `openid-client`, enabled by `AUTH_PROVIDERS=entraid`
- `req.user` is typed as `User` entity via `src/@types/express/index.d.ts`

## Integration Points

- **PostgreSQL** — main `dataSource` + separate `cubeDataSource` for DuckDB-built cube data (`src/db/`)
- **DuckDB** — cube building and data queries (`src/services/duckdb.ts`, `src/services/cube-builder.ts`)
- **Azure Blob / DataLake** — file storage abstracted behind `fileService` (`src/services/blob-storage.ts`, `src/services/datalake-storage.ts`)
- **Valkey (Redis)** — session store, started via `docker-compose`
- **ClamAV** — virus scanning for uploads
