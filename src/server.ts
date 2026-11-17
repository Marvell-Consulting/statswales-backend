import 'dotenv/config';
import 'reflect-metadata';

import { config } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initPassport } from './middleware/passport-auth';
import { dbManager } from './db/database-manager';
import { markStartupComplete, retryWhileDatabaseUnreachable } from './utils/startup';

// Without these, an unawaited promise rejection (e.g. an abandoned pg pool acquisition)
// terminates the process under Node's default --unhandled-rejections=throw, with no log
// line identifying the offending call site. Log and stay alive for rejections; log and
// exit for uncaught exceptions since the process state after one is undefined.
process.on('unhandledRejection', (reason: unknown, promise: Promise<unknown>) => {
  logger.error({ err: reason, promise }, 'Unhandled promise rejection');
});

process.on('uncaughtException', (err: Error, origin: string) => {
  logger.fatal({ err, origin }, 'Uncaught exception, process will exit');
  setTimeout(() => process.exit(1), 100).unref();
});

const PORT = config.backend.port;

// Listen before connecting to the database, so the startup and liveness probes pass while it connects or waits for
// a database that is restarting. /healthcheck/ready fails until startup is complete, so no traffic comes here first.
app.listen(PORT, () => {
  logger.info(`Listening on port ${PORT}, connecting to the database...`);
});

const start = async (): Promise<void> => {
  await retryWhileDatabaseUnreachable(async () => {
    await dbManager.initDataSources();
    if (config.database.runMigrationsOnStart) {
      await dbManager.runMigrations();
    }
  });
  await dbManager.initEntitySubscriber();
  await initPassport();
  markStartupComplete();

  logger.info(
    { event: 'app_boot', gitSha: config.build.gitSha, appEnv: config.env, port: PORT },
    `Server is running on port ${PORT}`
  );
};

start().catch(async (err) => {
  logger.error(err, 'Startup failed, process will exit');
  await dbManager.destroyDataSources().catch(() => undefined);
  process.exit(1);
});
