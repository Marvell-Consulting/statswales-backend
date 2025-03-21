import { Database } from 'duckdb-async';
import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { duckdb } from '../services/duckdb';
import { createEmptyFactTableInCube, loadFactTables } from '../services/cube-handler';
import { logger } from './logger';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';

export async function createEmptyCubeWithFactTable(dataset: Dataset): Promise<Database> {
  let endRevision: Revision;
  if (dataset.draftRevision) {
    endRevision = dataset.draftRevision;
  } else if (dataset.publishedRevision) {
    endRevision = dataset.publishedRevision;
  } else {
    throw new Error('No revision present on the dataset');
  }
  const quack = await duckdb();
  try {
    const { notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } = await createEmptyFactTableInCube(
      quack,
      dataset
    );
    await loadFactTables(quack, dataset, endRevision, factTableDef, dataValuesColumn, notesCodeColumn, factIdentifiers);
  } catch (error) {
    await quack.close();
    logger.error(error, `Something went wrong trying to load the fact table into DuckDB`);
    throw new FileValidationException('Fact table creation failed', FileValidationErrorType.FactTable, 500);
  }
  return quack;
}
