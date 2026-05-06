# Copilot Instructions — StatsWales Backend

These instructions are primarily for Copilot when **reviewing pull requests**. The aim is consistent, focused PRs that respect the project's layered architecture. Apply the rules below as review heuristics: read each changed file, ask which layer it belongs to, and flag anything that violates the layer's responsibility or the dependency direction.

## Project summary

This is the backend for a bilingual (English/Welsh) Welsh Government statistics platform. It ingests datasets and lookup tables, builds them into data cubes, and serves them via two APIs that share a single Express app: a public unauthenticated API (versioned under `/v1` and `/v2`, documented with Swagger) for finding and viewing published datasets, and an authenticated publisher API for creating, updating, and publishing them.

## Tech stack

- Node 24, TypeScript (strict)
- Express
- TypeORM + PostgreSQL
- DuckDB
- Azure Data Lake (storage)
- EntraID (auth)
- Jest (unit + integration)
- Prettier + ESLint

## PR scope and hygiene

Flag PRs that:

- Mix unrelated changes — e.g. the stated purpose is a bug fix but the diff also includes a refactor of an adjacent module, a rename across the codebase, or formatting churn in files that were not otherwise touched. Cross-cutting changes should be raised as separate PRs so they can be reviewed and reverted independently.
- Touch many files for a small functional change without explanation. Ask whether the surface area can be narrowed.
- Introduce new dependencies, framework choices, or architectural patterns without a note in the PR description explaining why.

When suggesting an improvement that is out of scope for the current PR, say so explicitly — recommend opening a follow-up rather than expanding this one.

## Code conventions to flag in review

- Functions that mutate their parameters when a pure function returning a new value would do.
- Raw promise chains where `async`/`await` would be clearer.
- New filenames that are not kebab-case (e.g. `dataset-status.ts`, `incoming-file-processor.ts` are correct).
- Multiple unrelated features in one file. One feature per file.
- Exported function names that do not use clear verb-led names: action verbs for commands (`createDataset`, `updateMetadata`) and query verbs for reads (`getDatasetById`, `listUserDatasets`, `findLatestRelease`). Avoid ambiguous names.
- Comments that restate what well-named code already says. Useful comments explain a non-obvious *why*.

## Directory layout (`src/`)

Each directory has a single responsibility. Code that does not fit a directory's responsibility belongs elsewhere.

| Directory       | Responsibility                                                                                  |
| --------------- | ----------------------------------------------------------------------------------------------- |
| `routes/`       | Express `Router` definitions: wire HTTP verb + path + middleware to a controller function.      |
| `controllers/`  | Express request handlers — `(req, res, next)`. Parse input, call services/repositories, respond.|
| `services/`     | Business logic and orchestration across repositories and external systems.                      |
| `repositories/` | TypeORM data access. Queries, finders, persistence. Return entities/DTOs.                       |
| `middleware/`   | Express middleware (`(req, res, next)`) for cross-cutting concerns (auth, streaming, timeouts). |
| `validators/`   | Pure input validation predicates and schemas. No side effects.                                  |
| `dtos/`         | Data transfer object shapes for HTTP transport and entity ↔ DTO mapping.                        |
| `entities/`     | TypeORM entity classes. Schema/relation definitions and entity-local invariants only.           |
| `extractors/`   | Helpers that extract typed values from source data columns during ingest.                       |
| `exceptions/`   | Typed error classes only.                                                                       |
| `enums/`        | Enum and runtime constant definitions.                                                          |
| `interfaces/`   | TypeScript interface definitions. Type-only.                                                    |
| `types/`        | Shared type aliases. Type-only.                                                                 |
| `utils/`        | Small, pure, stateless, framework-agnostic helpers reusable across layers.                      |
| `db/`           | TypeORM data source, database manager, connection lifecycle.                                    |
| `config/`       | Configuration loading and shape.                                                                |
| `migrations/`   | TypeORM migration files. Generated/hand-written schema changes only.                            |
| `seeders/`      | Seed data scripts.                                                                              |

## Layer rules

### Controllers (`src/controllers/`)

- **Must** export functions with the exact Express signature: `(req: Request, res: Response, next: NextFunction)` (async permitted).
- **Must** be limited to: extracting request data, validating input via `validators/`, calling `services/` or `repositories/`, mapping the result to a DTO, and sending the response (or calling `next(err)`).
- **Must not** contain business logic, raw SQL, TypeORM queries (`dataSource.*`, `getRepository`, `QueryBuilder`, `pgformat`, or entity method calls like `.save()`/`.find*()`/`.remove()`/`.delete()`), file I/O orchestration, cube building, virus scanning, or other domain work — delegate to a service or repository.
- **Must not** define generic helpers. If a helper has no `req`/`res` reference and is reusable, it belongs in `utils/`, `services/`, or `validators/`.

### Routes (`src/routes/`)

- **Must** only construct `Router` instances and bind verbs/paths to controller functions, optionally with middleware (`router.METHOD(path, [middleware,] controllerFn)`).
- **Must not** contain inline handlers with logic, data access, or conditionals.

### Services (`src/services/`)

- **Must** contain business logic, orchestration, and integration with external systems (DuckDB, Azure Data Lake, virus scanner, etc.).
- **Must not** reference `req`, `res`, `next`, or import Express types.
- **Must not** issue raw TypeORM queries that duplicate or bypass an existing repository — extend the repository instead.
- **Must not** return `Response` objects or write directly to the response stream, except in dedicated streaming/export helpers.

### Repositories (`src/repositories/`)

- **Must** contain TypeORM data access only: finders, query builders, persistence, transactions.
- **Must** return entities or DTOs — never `Response` objects or raw SQL strings.
- **Must not** contain business rules, HTTP concerns, or cross-service orchestration.
- **Must not** import from `controllers/` or `services/`.

### Middleware (`src/middleware/`)

- **Must** use the Express middleware signature `(req, res, next)` (or the 4-arg error variant).
- **Must** address a cross-cutting concern (auth, request shaping, streaming, rate limiting, etc.).
- **Must not** contain feature-specific business logic — that belongs in a service.

### Validators (`src/validators/`)

- **Must** be pure: take input, return a result/throw. No DB, no HTTP, no file I/O, no logging beyond trivial.

### DTOs (`src/dtos/`)

- **Must** describe transport shapes and provide entity ↔ DTO mapping.
- **Must not** contain business logic.

### Entities (`src/entities/`)

- **Must** be TypeORM entity classes describing schema and relations.
- **Should** contain only entity-local invariants. Cross-entity logic belongs in services.

### Utils (`src/utils/`)

- **Must** be small, pure, stateless, framework-agnostic helpers reusable across layers.
- **Must not** import from `controllers/`, `services/`, `repositories/`, or `routes/`. Entity imports are acceptable only for types.

### Exceptions (`src/exceptions/`)

- Typed `Error` subclasses only. No logic beyond constructing the error.

## Allowed dependency direction

Allowed forward imports:

- `routes/` → `controllers/`, `middleware/`
- `controllers/` → `services/`, `repositories/`
- `services/` → `repositories/`, other `services/`
- `repositories/` → `entities/`, `db/`
- `entities/` → other `entities/`

Any layer may import from `utils/`, `dtos/`, `enums/`, `exceptions/`, `interfaces/`, `types/`, and `validators/`. Flag any reverse-direction import introduced by the PR (e.g. a repository importing from a service, a util importing from a service or repository, a service importing from a controller).

## Review heuristics — quick checklist

For each changed file, ask:

- Is this file in the correct directory for its responsibility?
- Does each exported function fit the layer's allowed shape (signature, dependencies, return type)?
- Are there any HTTP concerns (`req`/`res`/`next`) leaking into a non-Express layer?
- Are there any TypeORM/SQL calls outside `repositories/`, `migrations/`, or `db/`?
- Does the PR's diff scope match its stated purpose, or has it pulled in unrelated changes?

When suggesting where new code should live, name the directory explicitly and give a one-line reason (e.g. "this belongs in `services/` because it orchestrates a repository call and has no HTTP concerns").

## Domain knowledge

- The first/initial revision of a dataset always has `revision_index = 1`.
- Subsequent revisions have `revision_index = 0` while in draft state, then take the previous revision index + 1 on approval.
