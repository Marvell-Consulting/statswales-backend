import { DataSource } from 'typeorm';

import { logger } from './logger';

// A Postgres session-level advisory lock, held on a single dedicated connection for the
// duration of `fn`. Guards against two overlapping executions of a job doing the same work at
// once (eg. a manual run overlapping a scheduled one). No TTL or heartbeat bookkeeping is needed -
// Postgres releases the lock itself if the connection drops, so it can never be left stuck held.
export async function withAdvisoryLock<T>(
  dataSource: DataSource,
  lockKey: number,
  fn: () => Promise<T>
): Promise<T | undefined> {
  const runner = dataSource.createQueryRunner();

  try {
    const [{ locked }]: [{ locked: boolean }] = await runner.query('SELECT pg_try_advisory_lock($1) AS locked', [
      lockKey
    ]);

    if (!locked) {
      logger.info(`advisory-lock: lock ${lockKey} is held elsewhere, skipping`);
      return undefined;
    }

    try {
      return await fn();
    } finally {
      await runner
        .query('SELECT pg_advisory_unlock($1)', [lockKey])
        .catch((err) => logger.error(err, `advisory-lock: failed to release lock ${lockKey}`));
    }
  } finally {
    await runner.release().catch((err) => logger.error(err, 'advisory-lock: failed to release query runner'));
  }
}
