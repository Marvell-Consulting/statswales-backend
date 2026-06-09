import 'dotenv/config';
import 'reflect-metadata';

import { config } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initPassport } from './middleware/passport-auth';
import { dbManager } from './db/database-manager';

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

Promise.resolve()
  .then(async () => {
    await dbManager.initDataSources();
    await dbManager.initEntitySubscriber();
    await initPassport();
  })
  .then(() => {
    app.listen(PORT, async () => {
      logger.info(
        { event: 'app_boot', gitSha: config.build.gitSha, appEnv: config.env, port: PORT },
        `Server is running on port ${PORT}`
      );
    });
  })
  .catch(async (err) => {
    logger.error(err);
    await dbManager.destroyDataSources();
    process.exit(1);
  });
