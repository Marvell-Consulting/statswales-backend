import os from 'node:os';
import path from 'node:path';
import { readdir, stat, unlink } from 'node:fs/promises';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';

import { logger } from '../utils/logger';
import { dbManager } from '../db/database-manager';
import { withAdvisoryLock } from '../utils/advisory-lock';
import { BuildLogRepository } from '../repositories/build-log';
import { DatasetRepository } from '../repositories/dataset';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';

// only these build types create a cube schema named after their own build id; bulk orchestrator
// builds (AllCubes/DraftCubes/AllFilterTables) have no schema of their own to drop
const SCHEMA_OWNING_BUILD_TYPES = [CubeBuildType.BaseCube, CubeBuildType.ValidationCube, CubeBuildType.FullCube];

// schema is renamed from the build id to the revision id once building finishes (see cube-builder.ts),
// so a build stuck in one of these earlier statuses still owns a schema named after its build id
const UNRENAMED_BUILD_STATUSES = [CubeBuildStatus.Queued, CubeBuildStatus.Building, CubeBuildStatus.SchemaRename];

// files written by the AV scanner (src/services/virus-scanner.ts) directly into the OS temp dir,
// named as 32 hex chars with no extension - anything else in the temp dir is left alone
const TMP_FILENAME_PATTERN = /^[0-9a-f]{32}$/;

// arbitrary constants identifying each job's advisory lock; only need to be unique among any
// other advisory locks this app takes out
const ORPHANED_BUILD_CLEANUP_LOCK_KEY = 8234178;
const MATERIALIZED_VIEW_CLEANUP_LOCK_KEY = 8234179;

// Runs behind an advisory lock since every app replica shares one Postgres database and would
// otherwise all race to reconcile (and drop cube schemas for) the same stuck builds concurrently.
export async function cleanupOrphanedCubeBuilds(staleAfterMs: number): Promise<void> {
  await withAdvisoryLock(dbManager.getPublisherDataSource(), ORPHANED_BUILD_CLEANUP_LOCK_KEY, async () => {
    const cutoff = new Date(Date.now() - staleAfterMs);
    const stuckBuilds = await BuildLogRepository.getStuckBuilds(cutoff);

    if (stuckBuilds.length === 0) {
      logger.info('cleanup: no orphaned cube builds found');
      return;
    }

    logger.warn(`cleanup: found ${stuckBuilds.length} orphaned cube build(s) started before ${cutoff.toISOString()}`);

    for (const build of stuckBuilds) {
      if (SCHEMA_OWNING_BUILD_TYPES.includes(build.type) && UNRENAMED_BUILD_STATUSES.includes(build.status)) {
        const runner = dbManager.getCubeDataSource().createQueryRunner();
        try {
          await runner.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE', build.id));
          logger.info(`cleanup: dropped orphaned cube schema for build ${build.id}`);
        } catch (err) {
          logger.error(err, `cleanup: failed to drop orphaned cube schema for build ${build.id}`);
        } finally {
          await runner.release().catch((err) => logger.error(err, 'cleanup: failed to release cube query runner'));
        }
      }

      build.completeBuild(
        CubeBuildStatus.Failed,
        undefined,
        'Build timed out and was reconciled by the nightly cleanup job'
      );
      await build
        .save()
        .catch((err) => logger.error(err, `cleanup: failed to persist failed status for build ${build.id}`));
    }
  });
}

export async function cleanupStaleTempFiles(staleAfterMs: number): Promise<void> {
  const tmpDir = os.tmpdir();
  const cutoff = Date.now() - staleAfterMs;

  let entries: string[];
  try {
    entries = await readdir(tmpDir);
  } catch (err) {
    logger.error(err, `cleanup: failed to read temp directory ${tmpDir}`);
    return;
  }

  let removed = 0;

  for (const entry of entries) {
    if (!TMP_FILENAME_PATTERN.test(entry)) continue;

    const filePath = path.join(tmpDir, entry);

    try {
      const stats = await stat(filePath);
      if (!stats.isFile() || stats.mtimeMs > cutoff) continue;
      await unlink(filePath);
      removed++;
    } catch {
      // may already have been cleaned up by the request that created it - ignore
    }
  }

  if (removed > 0) {
    logger.info(`cleanup: removed ${removed} stale temp file(s) from ${tmpDir}`);
  } else {
    logger.info('cleanup: no stale temp files found');
  }
}

// drops the materialized views (core_view_mat_<lang>) for revision cube schemas that are no
// longer a dataset's current draft or published revision. The underlying schema and its base
// tables are left in place - only the materialized view is removed - so this does not affect the
// public API (which only ever reads the current published revision), but it does mean an editor
// can no longer preview/download that specific superseded revision until its cube is rebuilt.
// Runs behind an advisory lock since every app replica shares one Postgres database and would
// otherwise all perform the same (harmless but wasteful) work concurrently.
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

      for (const { schemaname, matviewname } of staleMatviews) {
        try {
          await cubeRunner.query(pgformat('DROP MATERIALIZED VIEW IF EXISTS %I.%I', schemaname, matviewname));
        } catch (err) {
          logger.error(err, `cleanup: failed to drop materialized view ${schemaname}.${matviewname}`);
        }
      }
    } finally {
      await cubeRunner
        .release()
        .catch((err) => logger.error(err, 'cleanup: failed to release cube query runner'));
    }
  });
}

export async function runNightlyCleanup(staleBuildTimeoutMs: number, staleTempFileTimeoutMs: number): Promise<void> {
  logger.info('cleanup: starting nightly cleanup job');

  const results = await Promise.allSettled([
    cleanupOrphanedCubeBuilds(staleBuildTimeoutMs),
    cleanupStaleTempFiles(staleTempFileTimeoutMs),
    cleanupSupersededMaterializedViews()
  ]);

  for (const result of results) {
    if (result.status === 'rejected') {
      logger.error(result.reason, 'cleanup: a nightly cleanup task failed');
    }
  }

  logger.info('cleanup: nightly cleanup job complete');
}
