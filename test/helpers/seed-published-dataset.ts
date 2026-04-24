import { format as pgformat } from '@scaleleap/pg-format';

import { User } from '../../src/entities/user/user';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { RevisionMetadata } from '../../src/entities/dataset/revision-metadata';
import { RevisionTopic } from '../../src/entities/dataset/revision-topic';
import { Topic } from '../../src/entities/dataset/topic';
import { DataTable } from '../../src/entities/dataset/data-table';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { DataTableAction } from '../../src/enums/data-table-action';
import { FileType } from '../../src/enums/file-type';
import { cubeDataSource } from '../../src/db/cube-source';
import { createAllCubeFiles } from '../../src/services/cube-builder';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

export interface BilingualText {
  en: string;
  cy: string;
}

export interface FactColumnSpec {
  name: string;
  datatype: string; // e.g. 'BIGINT', 'DOUBLE PRECISION', 'VARCHAR'
}

export interface SeedPublishedDatasetOpts {
  user: User;
  datasetId: string;
  revisionId: string;
  dataTableId: string;
  title?: BilingualText;
  summary?: BilingualText;
  /** Shape of the fact table in the cube. Defaults to Area/Year/Data columns. */
  factColumns?: FactColumnSpec[];
  /** Generator for each row, given 0-based index. Defaults to values matching the default factColumns. */
  rowBuilder?: (i: number) => unknown[];
  rowCount?: number; // default 10
  topicIds?: number[];
  /** Revisions with publishAt in the past are "published". Defaults to 1 day ago. */
  publishAt?: Date;
  /** firstPublishedAt on the Dataset. Defaults to publishAt. */
  firstPublishedAt?: Date;
  /** Omit userGroup linkage (useful for tests that assert publisher block is absent). */
  skipUserGroup?: boolean;
}

const DEFAULT_FACT_COLUMNS: FactColumnSpec[] = [
  { name: 'AreaCode', datatype: 'VARCHAR' },
  { name: 'YearCode', datatype: 'BIGINT' },
  { name: 'Data', datatype: 'DOUBLE PRECISION' }
];

const defaultRowBuilder = (i: number): unknown[] => {
  const areas = ['W06000001', 'W06000002', 'W06000003'];
  const years = [2020, 2021, 2022, 2023];
  return [areas[i % areas.length], years[i % years.length], Math.round((i + 1) * 10.5 * 100) / 100];
};

/**
 * Seeds a published dataset end-to-end: Dataset + Revision (approved, publishAt in the past,
 * onlineCubeFilename set) + bilingual metadata + fact table in the cube schema + rows +
 * cube materialisation via createAllCubeFiles.
 *
 * Leaves the caller free to attach dimensions/lookup tables afterwards if a test needs them.
 */
export async function seedPublishedDataset(opts: SeedPublishedDatasetOpts): Promise<{
  datasetId: string;
  revisionId: string;
  dataTableId: string;
}> {
  const publishAt = opts.publishAt ?? new Date(Date.now() - ONE_DAY_MS);
  const firstPublishedAt = opts.firstPublishedAt ?? publishAt;
  const title = opts.title ?? { en: 'Test Published Dataset', cy: 'Set Ddata Prawf Wedi Cyhoeddi' };
  const summary = opts.summary ?? { en: 'Integration test fixture', cy: 'Ffynhonnell integreiddio' };
  const factColumns = opts.factColumns ?? DEFAULT_FACT_COLUMNS;
  const rowBuilder = opts.rowBuilder ?? defaultRowBuilder;
  const rowCount = opts.rowCount ?? 10;

  await Dataset.create({
    id: opts.datasetId,
    createdBy: opts.user,
    userGroupId: opts.skipUserGroup ? undefined : opts.user?.groupRoles[0]?.groupId,
    firstPublishedAt,
    factTable: factColumns.map((col, idx) =>
      FactTableColumn.create({
        columnName: col.name,
        columnIndex: idx,
        columnType: FactTableColumnType.Unknown,
        columnDatatype: col.datatype
      })
    )
  }).save();

  await Revision.create({
    id: opts.revisionId,
    datasetId: opts.datasetId,
    createdBy: opts.user,
    revisionIndex: 1,
    approvedAt: publishAt,
    approvedBy: opts.user,
    publishAt,
    onlineCubeFilename: `${opts.revisionId}.duckdb`,
    metadata: [
      RevisionMetadata.create({ language: 'en-GB', title: title.en, summary: summary.en }),
      RevisionMetadata.create({ language: 'cy-GB', title: title.cy, summary: summary.cy })
    ],
    dataTable: DataTable.create({
      id: opts.dataTableId,
      filename: `${opts.dataTableId}.csv`,
      originalFilename: 'test-data.csv',
      hash: `test-hash-${opts.dataTableId}`,
      action: DataTableAction.Add,
      fileType: FileType.Csv,
      mimeType: 'text/csv',
      dataTableDescriptions: factColumns.map((col, idx) => ({
        columnName: col.name,
        columnIndex: idx,
        columnDatatype: col.datatype,
        factTableColumn: col.name
      }))
    })
  }).save();

  await Dataset.update(opts.datasetId, {
    startRevisionId: opts.revisionId,
    endRevisionId: opts.revisionId,
    publishedRevisionId: opts.revisionId
  });

  if (opts.topicIds && opts.topicIds.length > 0) {
    await RevisionTopic.save(
      opts.topicIds.map((topicId) => RevisionTopic.create({ revisionId: opts.revisionId, topicId }))
    );
  }

  const cubeDB = cubeDataSource.createQueryRunner();
  try {
    await cubeDB.query(pgformat('CREATE SCHEMA IF NOT EXISTS %I;', opts.revisionId));
    const columnDefs = factColumns.map((col) => `"${col.name}" ${col.datatype}`).join(', ');
    await cubeDB.query(`CREATE TABLE data_tables."${opts.dataTableId}" (${columnDefs});`);

    if (rowCount > 0) {
      const BATCH_SIZE = 500;
      for (let i = 0; i < rowCount; i += BATCH_SIZE) {
        const end = Math.min(i + BATCH_SIZE, rowCount);
        const rows: unknown[][] = [];
        for (let j = i; j < end; j++) rows.push(rowBuilder(j));
        const valuesSql = rows.map((row) => `(${row.map((v) => pgformat('%L', v)).join(',')})`).join(',');
        await cubeDB.query(`INSERT INTO data_tables."${opts.dataTableId}" VALUES ${valuesSql};`);
      }
    }
  } finally {
    await cubeDB.release();
  }

  await createAllCubeFiles(opts.datasetId, opts.revisionId);

  return { datasetId: opts.datasetId, revisionId: opts.revisionId, dataTableId: opts.dataTableId };
}

/**
 * Adds a subsequent published revision to an already-published dataset. Useful for history tests.
 * Does not rebuild the cube; pass rowCount: 0 to skip inserting fact rows, or provide your own.
 */
export async function addPublishedRevision(opts: {
  user: User;
  datasetId: string;
  revisionId: string;
  dataTableId: string;
  previousRevisionId: string;
  revisionIndex: number;
  title?: BilingualText;
  summary?: BilingualText;
  publishAt?: Date;
  factColumns?: FactColumnSpec[];
  rowBuilder?: (i: number) => unknown[];
  rowCount?: number;
  buildCube?: boolean;
}): Promise<Revision> {
  const publishAt = opts.publishAt ?? new Date(Date.now() - ONE_DAY_MS / 2);
  const title = opts.title ?? { en: 'Updated Revision', cy: 'Adolygiad Wedi Diweddaru' };
  const summary = opts.summary ?? { en: 'Second revision', cy: 'Ail adolygiad' };
  const factColumns = opts.factColumns ?? DEFAULT_FACT_COLUMNS;

  const revision = await Revision.create({
    id: opts.revisionId,
    datasetId: opts.datasetId,
    createdBy: opts.user,
    revisionIndex: opts.revisionIndex,
    previousRevisionId: opts.previousRevisionId,
    approvedAt: publishAt,
    approvedBy: opts.user,
    publishAt,
    onlineCubeFilename: `${opts.revisionId}.duckdb`,
    metadata: [
      RevisionMetadata.create({ language: 'en-GB', title: title.en, summary: summary.en }),
      RevisionMetadata.create({ language: 'cy-GB', title: title.cy, summary: summary.cy })
    ],
    dataTable: DataTable.create({
      id: opts.dataTableId,
      filename: `${opts.dataTableId}.csv`,
      originalFilename: 'test-data-update.csv',
      hash: `test-hash-${opts.dataTableId}`,
      action: DataTableAction.Add,
      fileType: FileType.Csv,
      mimeType: 'text/csv',
      dataTableDescriptions: factColumns.map((col, idx) => ({
        columnName: col.name,
        columnIndex: idx,
        columnDatatype: col.datatype,
        factTableColumn: col.name
      }))
    })
  }).save();

  await Dataset.update(opts.datasetId, {
    endRevisionId: revision.id,
    publishedRevisionId: revision.id
  });

  if (opts.buildCube) {
    const rowBuilder = opts.rowBuilder ?? defaultRowBuilder;
    const rowCount = opts.rowCount ?? 10;
    const cubeDB = cubeDataSource.createQueryRunner();
    try {
      await cubeDB.query(pgformat('CREATE SCHEMA IF NOT EXISTS %I;', revision.id));
      const columnDefs = factColumns.map((col) => `"${col.name}" ${col.datatype}`).join(', ');
      await cubeDB.query(`CREATE TABLE data_tables."${opts.dataTableId}" (${columnDefs});`);
      if (rowCount > 0) {
        const rows: unknown[][] = [];
        for (let j = 0; j < rowCount; j++) rows.push(rowBuilder(j));
        const valuesSql = rows.map((row) => `(${row.map((v) => pgformat('%L', v)).join(',')})`).join(',');
        await cubeDB.query(`INSERT INTO data_tables."${opts.dataTableId}" VALUES ${valuesSql};`);
      }
    } finally {
      await cubeDB.release();
    }
    await createAllCubeFiles(opts.datasetId, revision.id);
  }

  return revision;
}

/**
 * Creates a Topic row. Topics need to exist before they can be attached to a revision.
 * resetDatabase() truncates the topic table, so tests must re-create whatever topics they need.
 */
export async function seedTopic(opts: { id: number; path: string; nameEN: string; nameCY: string }): Promise<Topic> {
  // Topic.id is declared as @PrimaryGeneratedColumn, so TypeORM strips explicit ids from
  // save()/insert(). Use raw SQL so the caller-supplied id is honoured, then bump the sequence
  // past the manually-inserted id so future auto-generated rows don't collide.
  const repo = Topic.getRepository();
  await repo.query('INSERT INTO topic (id, path, name_en, name_cy) VALUES ($1, $2, $3, $4)', [
    opts.id,
    opts.path,
    opts.nameEN,
    opts.nameCY
  ]);
  await repo.query(`SELECT setval(pg_get_serial_sequence('topic', 'id'), GREATEST((SELECT MAX(id) FROM topic), 1))`);
  return (await repo.findOneByOrFail({ id: opts.id })) as Topic;
}
