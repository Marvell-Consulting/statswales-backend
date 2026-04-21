import { dbManager } from '../../src/db/database-manager';

// Registered at import time (synchronous, before any test runs) so jest-circus allows it.
// Each test file gets its own module instance, so each file gets its own afterAll registration.
afterAll(async () => {
  if (dbManager.getAppDataSource().isInitialized) {
    await dbManager.destroyDataSources();
  }
});

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
