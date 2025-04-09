import { Database } from 'duckdb-async';
import { Dataset } from '../entities/dataset/dataset';
import { duckdb } from '../services/duckdb';
import { createEmptyFactTableInCube, loadFactTables } from '../services/cube-handler';
import { logger } from './logger';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { getFileService } from './get-file-service';
import tmp from 'tmp';
import fs from 'fs';

export async function createEmptyCubeWithFactTable(dataset: Dataset): Promise<Database> {
  const endRevision = dataset.draftRevision;
  if (!endRevision) {
    throw new Error('No draft revision present on the dataset');
  }
  let quack: Database;
  const filename = tmp.tmpNameSync({ postfix: '.duckdb' });
  logger.debug(`endRevision.onlineCubeFilename = ${endRevision.onlineCubeFilename}`);
  if (endRevision.onlineCubeFilename && endRevision.onlineCubeFilename.includes('protocube')) {
    logger.debug('Loading protocube file from blob storage');
    const fileService = getFileService();
    const cubeFile = await fileService.loadBuffer(endRevision.onlineCubeFilename, dataset.id);
    fs.writeFileSync(filename, cubeFile);
    quack = await duckdb(filename);
  } else {
    logger.warn('No protocube file found, creating a new cube... this could take a few seconds');
    quack = await duckdb(filename);
    try {
      const { notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } = await createEmptyFactTableInCube(
        quack,
        dataset
      );
      await loadFactTables(
        quack,
        dataset,
        endRevision,
        factTableDef,
        dataValuesColumn,
        notesCodeColumn,
        factIdentifiers
      );
    } catch (error) {
      await quack.close();
      logger.error(error, `Something went wrong trying to load the fact table into DuckDB`);
      throw new FileValidationException('Fact table creation failed', FileValidationErrorType.FactTable, 500);
    }
  }
  return quack;
}
