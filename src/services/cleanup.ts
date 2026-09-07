import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';

import { logger } from '../utils/logger';
import { dbManager } from '../db/database-manager';
import { withAdvisoryLock } from '../utils/advisory-lock';
import { DatasetRepository } from '../repositories/dataset';

// arbitrary constant identifying this job's advisory lock; only needs to be unique among any
// other advisory locks this app takes out
const MATERIALIZED_VIEW_CLEANUP_LOCK_KEY = 8234179;

// drops the materialized views (core_view_mat_<lang>) for revision cube schemas that are no
// longer a dataset's current draft or published revision. The underlying schema and its base
// tables are left in place - only the materialized view is removed - so this does not affect the
// public API (which only ever reads the current published revision), but it does mean an editor
// can no longer preview/download that specific superseded revision until its cube is rebuilt.
// Runs behind an advisory lock in case a manual run overlaps a scheduled one.
export async function cleanupSupersededMaterializedViews(): Promise<void> {
  await withAdvisoryLock(dbManager.getPublisherDataSource(), MATERIALIZED_VIEW_CLEANUP_LOCK_KEY, async () => {
    const keepSchemas = new Set(await DatasetRepository.getActiveRevisionIds());

    const cubeRunner = dbManager.getCubeDataSource().createQueryRunner();
    try {
      const matviews: { schemaname: string; matviewname: string }[] = await cubeRunner.query(
        `SELECT schemaname, matviewname FROM pg_matviews WHERE matviewname LIKE $1 ESCAPE '\\'`,
        ['core\\_view\\_mat\\_%']
      );

      const staleMatviews = matviews.filter((matview) => !keepSchemas.has(matview.schemaname));

      if (staleMatviews.length === 0) {
        logger.info('cleanup: no superseded cube materialized views found');
        return;
      }

      const staleSchemaCount = new Set(staleMatviews.map((matview) => matview.schemaname)).size;
      logger.warn(
        `cleanup: dropping ${staleMatviews.length} materialized view(s) across ${staleSchemaCount} superseded revision(s)`
      );

      let failureCount = 0;

      // errors are caught per-view so one bad drop doesn't stop the rest of the sweep, but any
      // failure is still tracked and thrown once every view has been attempted, so the job as a
      // whole is marked as failed rather than silently succeeding with views left undropped
      for (const { schemaname, matviewname } of staleMatviews) {
        try {
          await cubeRunner.query(pgformat('DROP MATERIALIZED VIEW IF EXISTS %I.%I CASCADE', schemaname, matviewname));
        } catch (err) {
          failureCount++;
          logger.error(err, `cleanup: failed to drop materialized view ${schemaname}.${matviewname}`);
        }
      }

      if (failureCount > 0) {
        throw new Error(`cleanup: failed to drop ${failureCount} of ${staleMatviews.length} materialized view(s)`);
      }
    } finally {
      await cubeRunner.release().catch((err) => logger.error(err, 'cleanup: failed to release cube query runner'));
    }
  });
}
