# CLAUDE.md

## Summary

This is the frontend for a bilingual (English/Welsh) Welsh Government statistics platform.

## Tech Stack

- Node 24
- TypeScript
- JSX / TSX
- GOV.UK Design System
- Jest (unit tests)
- PlayWright (e2e tests)

## Available Tools

These tools are installed globally on the system and can be used via CLI commands.
- GitHub CLI `gh`
- UUID generation `uuidgen | tr '[:upper:]' '[:lower:]'`
- TypeScript Language Server (TypeScript LSP)
- **agent-browser** — browser automation CLI (`agent-browser <command>`); use for visual/e2e testing tasks; prefer over Playwright MCP as it uses ~10x fewer tokens
- **Atlassian CLI** — if `SW-<number>` is mentioned when discussing work, this is a Jira ticket id; fetch it with `acli jira workitem view SW-<number>`. Check auth with `acli auth status`; if unauthorized run `acli auth login`.

## Code Navigation

Prefer the **TypeScript LSP** (via `list_code_usages`) over `grep` or ad-hoc Python scripts when finding references,
usages, or definitions. The LSP is precise, understands types and scope, and won't produce false positives from
comments, strings, or coincidental name matches. Use `grep` only when searching for patterns the LSP can't express
(e.g. raw string literals, file content searches).

## Architecture

**Two Express 5 servers** from one TypeScript codebase:

- **Consumer** (`src/consumer/`) — public-facing: browse topics, search/view/filter/download published datasets (port 3100)
- **Publisher** (`src/publisher/`) — authenticated CMS: create/upload/configure/publish/update datasets (port 3000)
- **Shared** (`src/shared/`) — config, middleware, routes, DTOs, enums, i18n, views, utils

**Views are server-side rendered** using `express-react-views` (.jsx files) with React 16 — no client-side hydration.
Views use GOV.UK Frontend CSS classes as a base with additional modifications for Gov.Wales styling.

### i18n

Path-based detection (`/:lang` = `en-GB` or `cy-GB`). Translation files: `src/shared/i18n/en.json`, `src/shared/i18n/cy.json`.

### Integration Points

- **StatsWales Backend** — all data via `PublisherApi`/`ConsumerApi`; base URL from config
- **Azure EntraID** — used for publisher authentication in all envs except CI which uses a form login with pre-defined test users

## Testing Patterns

- **Integration tests** (`tests/`): import the real Express app and drive with `supertest`
- **Backend mocking**: MSW (`tests/mocks/backend.ts`) intercepts HTTP calls; lifecycle in `beforeAll`/`afterEach`/`afterAll`
- **API unit tests**: spy on `global.fetch` with `jest.spyOn`
- **Fixtures**: `tests/mocks/fixtures.ts`
- **Env vars**: seeded in `tests/.jest/set-env-vars.ts`
- **E2E**: Playwright in `tests-e2e/`, runs separately
- **E2E Welsh coverage**: A single "Can switch to Welsh" test per page is sufficient. Don't create additional tests that recheck the same functionality but in Welsh.

## Domain Knowledge

- The first/initial revision of a dataset always has `revision_index = 1`.
- Subsequent revisions have `revision_index = 0` while in draft state, then take the previous revision index + 1 on approval.

## Creating Pull Requests

Always write the PR body to a temporary file and pass it with `--body-file` — never pass a multiline body directly on the CLI, as the shell will hang in an uncloseable heredoc state:

```bash
cat > /tmp/pr-body.md << 'EOF'
... body content ...
EOF
gh pr create --title "..." --body-file /tmp/pr-body.md --base main
```

## Memory

Project-specific memory lives in `.claude/memory/` (gitignored). Start with `MEMORY.md` there for the index, then load topic files as relevant. Always read from and write to those files instead of the default auto memory path.

## Final Steps

Always run `npm run check` as a final step to ensure the code is properly formatted, can build and that the tests pass.
Fix any issues before continuing.

Never commit or push changes — the user handles all git commits manually.
