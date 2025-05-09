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
  const draftRevision = dataset.draftRevision;
  const buildLog: string[] = [];

  if (!draftRevision) {
    throw new Error('No draft revision present on the dataset');
  }

  let quack: Database;
  const filename = tmp.tmpNameSync({ postfix: '.duckdb' });

  if (draftRevision.onlineCubeFilename && draftRevision.onlineCubeFilename.includes('protocube')) {
    const fileService = getFileService();
    const cubeFile = await fileService.loadBuffer(draftRevision.onlineCubeFilename, dataset.id);
    fs.writeFileSync(filename, cubeFile);
    quack = await duckdb(filename);
  } else {
    logger.warn('No protocube file found, creating a new cube... this could take a few seconds');
    quack = await duckdb(filename);
    try {
      const { notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } = await createEmptyFactTableInCube(
        quack,
        dataset,
        buildLog
      );
      await loadFactTables(
        quack,
        dataset,
        draftRevision,
        factTableDef,
        dataValuesColumn,
        notesCodeColumn,
        factIdentifiers,
        buildLog
      );
    } catch (error) {
      await quack.close();
      logger.error(error, `Something went wrong trying to load the fact table into DuckDB`);
      throw new FileValidationException('Fact table creation failed', FileValidationErrorType.FactTable, 500);
    }
  }
  return quack;
}
