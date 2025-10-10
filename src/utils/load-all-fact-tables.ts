import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { DataTable } from '../entities/dataset/data-table';
import { logger } from './logger';
import { cleanupNotesCodeColumn, createPrimaryKeyOnFactTable, dataTableActions } from '../services/cube-handler';
import { TransactionBlock } from '../interfaces/transaction-block';
import { BuildStage } from '../enums/build-stage';

export function loadAllFactTables(
  dataset: Dataset,
  endRevision: Revision,
  buildId: string,
  factTableDef: string[],
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  factIdentifiers: FactTableColumn[],
  factTableCompositeKey: string[]
): TransactionBlock {
  const buildStatements: string[] = ['BEGIN TRANSACTION;'];
  logger.debug('Finding all fact tables for this revision and those that came before');
  const allFactTables: DataTable[] = [];
  if (endRevision.revisionIndex && endRevision.revisionIndex > 0) {
    // If we have a revision index we start here
    const validRevisions = dataset.revisions.filter(
      (rev) => rev.revisionIndex <= endRevision.revisionIndex && rev.revisionIndex > 0
    );
    validRevisions.forEach((revision) => {
      if (revision.dataTable) allFactTables.push(revision.dataTable);
    });
  } else {
    logger.debug('Must be a draft revision, so we need to find all revisions before this one');
    // If we don't have a revision index, we need to find the previous revision to this one that does
    if (endRevision.dataTable) {
      logger.debug('Adding end revision to list of fact tables');
      allFactTables.push(endRevision.dataTable);
    }
    const validRevisions = dataset.revisions.filter((rev) => rev.revisionIndex > 0);
    validRevisions.forEach((revision) => {
      if (revision.dataTable) allFactTables.push(revision.dataTable);
    });
  }

  const allDataTables = allFactTables.reverse().sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime());
  for (const dataTable of allDataTables) {
    buildStatements.push(
      ...dataTableActions(buildId, dataTable, factTableDef, notesCodeColumn, dataValuesColumn, factIdentifiers)
    );
  }
  buildStatements.push(cleanupNotesCodeColumn(buildId, notesCodeColumn));
  buildStatements.push(createPrimaryKeyOnFactTable(buildId, factTableCompositeKey));
  buildStatements.push('END TRANSACTION;');

  return {
    buildStage: BuildStage.FactTable,
    statements: buildStatements
  };
}
