// Sets per-worker DB name and DuckDB tmpdir before any app module is imported.
// Loaded via `setupFiles` in the integration project so it runs first.
const workerId = process.env.JEST_WORKER_ID ?? '1';
process.env.TEST_DB_DATABASE = `statswales-backend-test-${workerId}`;
process.env.TMPDIR = `/tmp/statswales-test-worker-${workerId}`;
