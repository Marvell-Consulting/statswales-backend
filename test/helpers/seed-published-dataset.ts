import { randomUUID } from 'node:crypto';

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
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionMetadata } from '../../src/entities/dataset/dimension-metadata';
import { LookupTable } from '../../src/entities/dataset/lookup-table';
import { Measure } from '../../src/entities/dataset/measure';
import { DimensionType } from '../../src/enums/dimension-type';
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
  /**
   * Classifies this column for the cube builder. Defaults to Unknown.
   * Set one column to DataValues so the cube builds as a FullCube (which triggers
   * setupDimensions and populates filter_table) rather than a BaseCube.
   */
  columnType?: FactTableColumnType;
}

export interface LookupRow {
  /** Value of the fact-table column this row maps — e.g. 'W06000001' for AreaCode. */
  reference: string | number;
  /**
   * Language short code — 'en' or 'cy'. The helper expands this to the lowercase locale
   * ('en-gb' / 'cy-gb') used by the cube-builder's filter_table WHERE clause.
   * Each reference should appear for every language.
   */
  language: 'en' | 'cy';
  description: string;
  sortOrder?: number;
  hierarchy?: string | null;
  notes?: string | null;
}

const LOCALE_FOR_LANG: Record<'en' | 'cy', string> = {
  en: 'en-gb',
  cy: 'cy-gb'
};

export interface DimensionSpec {
  /** Fact-table column name being described by this dimension (must also appear in factColumns). */
  factTableColumn: string;
  /** Dimension type (LookupTable, DatePeriod, Numeric, ...). Defaults to LookupTable. */
  type?: DimensionType;
  /** Required for LookupTable-like types. Fixed ID so tests can reference it. */
  lookupTableId?: string;
  /** Postgres type for the reference column in the lookup table — must match the fact column. */
  referenceDatatype?: string;
  /** Rows to populate the lookup table with. Provide each reference in both 'en' and 'cy'. */
  lookupRows?: LookupRow[];
  /** Optional human-readable name for the dimension. Used as the column label in filter_table. */
  name?: BilingualText;
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
  /** Lookup-backed dimensions. Required for v2 filter/pivot tests that resolve columns via filter_table. */
  dimensions?: DimensionSpec[];
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
        columnType: col.columnType ?? FactTableColumnType.Unknown,
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

  // If the fact table declares a Measure column, wire up a Measure entity so the cube-builder's
  // setupMeasuresAndDataValues path doesn't dereference a null `dataset.measure`.
  const measureColumn = factColumns.find((c) => c.columnType === FactTableColumnType.Measure);
  if (measureColumn) {
    await Measure.create({
      dataset: { id: opts.datasetId } as Dataset,
      factTableColumn: measureColumn.name,
      joinColumn: null,
      extractor: null,
      lookupTable: null,
      measureTable: null
    }).save();
  }

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

    if (opts.dimensions && opts.dimensions.length > 0) {
      await seedDimensions(opts.datasetId, factColumns, opts.dimensions, cubeDB);
    }
  } finally {
    await cubeDB.release();
  }

  // awaitMaterialisation=true: tests need core_view_en to exist before they query it.
  // The default fire-and-forget mode races the POST and 500s on slower CI runners.
  await createAllCubeFiles(opts.datasetId, opts.revisionId, undefined, undefined, undefined, true);

  return { datasetId: opts.datasetId, revisionId: opts.revisionId, dataTableId: opts.dataTableId };
}

/**
 * Creates Dimension + LookupTable entities and populates `lookup_tables.<id>` in the cube with
 * rows in the SW3 format expected by cube-builder (createLookupTableDimension copies that table
 * verbatim into the build schema and then INSERTs into filter_table from it).
 *
 * Must be invoked BEFORE createAllCubeFiles so the build reads the dimensions from the dataset
 * and the lookup_tables.<id> rows are in place when the cube is materialised.
 */
async function seedDimensions(
  datasetId: string,
  factColumns: FactColumnSpec[],
  dimensions: DimensionSpec[],
  cubeDB: { query: (sql: string) => Promise<unknown> }
): Promise<void> {
  for (const spec of dimensions) {
    const factCol = factColumns.find((c) => c.name === spec.factTableColumn);
    if (!factCol) {
      throw new Error(
        `seedDimensions: dimension.factTableColumn "${spec.factTableColumn}" is not declared in factColumns`
      );
    }
    const type = spec.type ?? DimensionType.LookupTable;
    const lookupTableId = spec.lookupTableId ?? randomUUID();
    const referenceDatatype = spec.referenceDatatype ?? factCol.datatype;

    const lookupTable = await LookupTable.create({
      id: lookupTableId,
      filename: `${lookupTableId}.csv`,
      originalFilename: 'test-lookup.csv',
      hash: `test-hash-${lookupTableId}`,
      mimeType: 'text/csv',
      fileType: FileType.Csv,
      isStatsWales2Format: false
    }).save();

    const dimension = await Dimension.create({
      datasetId,
      type,
      factTableColumn: spec.factTableColumn,
      joinColumn: null,
      isSliceDimension: false,
      extractor: null,
      lookupTable
    }).save();

    const name = spec.name ?? { en: spec.factTableColumn, cy: spec.factTableColumn };
    await DimensionMetadata.save([
      DimensionMetadata.create({ id: dimension.id, language: 'en-GB', name: name.en }),
      DimensionMetadata.create({ id: dimension.id, language: 'cy-GB', name: name.cy })
    ]);

    await cubeDB.query(
      pgformat(
        `CREATE TABLE lookup_tables.%I (
           %I ${referenceDatatype},
           language VARCHAR(5),
           description TEXT,
           hierarchy VARCHAR,
           sort_order INTEGER,
           notes TEXT
         );`,
        lookupTableId,
        spec.factTableColumn
      )
    );

    const rows = spec.lookupRows ?? [];
    if (rows.length > 0) {
      const valuesSql = rows
        .map((r) =>
          pgformat(
            '(%L, %L, %L, %L, %L, %L)',
            r.reference,
            LOCALE_FOR_LANG[r.language],
            r.description,
            r.hierarchy ?? null,
            r.sortOrder ?? null,
            r.notes ?? null
          )
        )
        .join(',');
      await cubeDB.query(pgformat('INSERT INTO lookup_tables.%I VALUES ', lookupTableId) + valuesSql + ';');
    }
  }
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
    await createAllCubeFiles(opts.datasetId, revision.id, undefined, undefined, undefined, true);
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
