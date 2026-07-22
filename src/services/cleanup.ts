import os from 'node:os';
import path from 'node:path';
import { readdir, stat, unlink } from 'node:fs/promises';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';

import { logger } from '../utils/logger';
import { dbManager } from '../db/database-manager';
import { BuildLogRepository } from '../repositories/build-log';
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

export async function cleanupOrphanedCubeBuilds(staleAfterMs: number): Promise<void> {
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
        void runner.release();
      }
    }

    build.completeBuild(
      CubeBuildStatus.Failed,
      undefined,
      'Build timed out and was reconciled by the nightly cleanup job'
    );
    await build.save();
  }
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

export async function runNightlyCleanup(staleBuildTimeoutMs: number, staleTempFileTimeoutMs: number): Promise<void> {
  logger.info('cleanup: starting nightly cleanup job');

  const results = await Promise.allSettled([
    cleanupOrphanedCubeBuilds(staleBuildTimeoutMs),
    cleanupStaleTempFiles(staleTempFileTimeoutMs)
  ]);

  for (const result of results) {
    if (result.status === 'rejected') {
      logger.error(result.reason, 'cleanup: a nightly cleanup task failed');
    }
  }

  logger.info('cleanup: nightly cleanup job complete');
}
