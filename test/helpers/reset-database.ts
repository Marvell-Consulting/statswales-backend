import { dbManager } from '../../src/db/database-manager';

// Registered at import time (synchronous, before any test runs) so jest-circus allows it.
// Each test file gets its own module instance, so each file gets its own afterAll registration.
afterAll(async () => {
  // Drain any in-flight queries before tearing down the pools. The cube-builder fires its
  // validation-cube cleanup as an unawaited `void cleanUpValidationCube(...)` (DROP SCHEMA CASCADE)
  // in src/services/revision.ts, so it can still be running when a test ends. Destroying the data
  // source mid-query surfaces as "Connection terminated"; letting it race resetDatabase's own
  // DROP SCHEMA surfaces as `relation "<id>.core_view_en" does not exist`.
  await waitForIdleConnections();
  await dbManager.destroyDataSources();
});

/**
 * Waits until this worker's database has no active queries other than our own, so that fire-and-forget
 * background work (e.g. the cube-builder's unawaited validation-cube cleanup) has settled before we
 * reset schemas or destroy the connection pools. Best-effort: returns on timeout or query error rather
 * than failing the suite. Both data sources point at the same per-worker DB, so polling pg_stat_activity
 * on the app pool also covers in-flight work on the cube pool.
 */
async function waitForIdleConnections(timeoutMs = 10000, intervalMs = 50): Promise<void> {
  if (!dbManager.getAppDataSource().isInitialized) return;
  const deadline = Date.now() + timeoutMs;
  const qr = dbManager.getAppDataSource().createQueryRunner();
  try {
    while (Date.now() < deadline) {
      const rows: { active: number }[] = await qr.query(`
        SELECT count(*)::int AS active
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
          AND state = 'active'
      `);
      if ((rows[0]?.active ?? 0) === 0) return;
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  } catch {
    // best-effort drain — fall through to teardown/reset
  } finally {
    // release() can itself reject (e.g. the connection already dropped); swallow it so the
    // best-effort drain never fails the suite.
    try {
      await qr.release();
    } catch {
      // ignore
    }
  }
}

/**
 * Initialises the dbManager and runs pending migrations for this test file's module instance.
 * Safe to call from every integration test's beforeAll — idempotent within a single test file.
 */
export async function ensureWorkerDataSources(): Promise<void> {
  if (dbManager.getAppDataSource().isInitialized) return;
  await dbManager.initDataSources();
  await dbManager.getAppDataSource().runMigrations();
}

/**
 * Fast reset between integration test suites.
 * Truncates all public tables (excluding the TypeORM migrations tracking table) and
 * drops/recreates the dynamic cube schemas so the next suite starts with a clean slate.
 */
export async function resetDatabase(): Promise<void> {
  // Let any unawaited cube-builder cleanup finish before we drop the dynamic schemas, otherwise the
  // two DROP SCHEMA CASCADE statements race on the same UUID schema.
  await waitForIdleConnections();
  const qr = dbManager.getAppDataSource().createQueryRunner();
  try {
    const tables: { tablename: string }[] = await qr.query(`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public' AND tablename != 'migrations'
    `);
    if (tables.length > 0) {
      const tableList = tables.map((t) => `"${t.tablename}"`).join(', ');
      await qr.query(`TRUNCATE TABLE ${tableList} RESTART IDENTITY CASCADE`);
    }

    // Drop dynamic revision schemas (UUID-named) created by cube-builder
    const schemas: { nspname: string }[] = await qr.query(`
      SELECT nspname FROM pg_namespace
      WHERE nspname NOT IN ('public', 'pg_catalog', 'information_schema', 'pg_toast', 'data_tables', 'lookup_tables')
      AND nspname NOT LIKE 'pg_%'
    `);
    for (const { nspname } of schemas) {
      await qr.query(`DROP SCHEMA "${nspname}" CASCADE`);
    }

    // Recreate the fixed cube schemas
    await qr.query(`DROP SCHEMA IF EXISTS data_tables CASCADE`);
    await qr.query(`CREATE SCHEMA data_tables`);
    await qr.query(`DROP SCHEMA IF EXISTS lookup_tables CASCADE`);
    await qr.query(`CREATE SCHEMA lookup_tables`);
  } finally {
    await qr.release();
  }
}
