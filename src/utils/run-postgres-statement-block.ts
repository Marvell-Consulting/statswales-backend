import { dbManager } from '../db/database-manager';
import { logger } from './logger';

export async function runQueryBlockInPostgres(statements: string[]): Promise<void> {
  const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    logger.trace(`Running queries:\n\n${statements.join('\n')}\n\n`);
    await queryRunner.query(statements.join('\n'));
  } catch (error) {
    logger.error(error, 'Something went wrong to trying to run statement block');
    throw error;
  } finally {
    void queryRunner.release();
  }
}
