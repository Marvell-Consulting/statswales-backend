import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';
import { format as pgformat } from '@scaleleap/pg-format';

import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { DimensionType } from '../../src/enums/dimension-type';
import { DimensionMetadata } from '../../src/entities/dataset/dimension-metadata';
import { User } from '../../src/entities/user/user';
import { DataTable } from '../../src/entities/dataset/data-table';
import { FileType } from '../../src/enums/file-type';
import { extractTableInformation } from '../../src/services/csv-processor';
import { DataTableAction } from '../../src/enums/data-table-action';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { LookupTable } from '../../src/entities/dataset/lookup-table';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { RevisionMetadata } from '../../src/entities/dataset/revision-metadata';
import { DimensionRepository } from '../../src/repositories/dimension';
import { DatasetRepository } from '../../src/repositories/dataset';
import { duckdb, linkToPostgres, linkToPostgresDataTables } from '../../src/services/duckdb';
import { logger } from '../../src/utils/logger';
import { Readable } from 'node:stream';

export async function createSmallDataset(
  datasetId: string,
  revisionId: string,
  importId: string,
  user: User,
  testFilePath = '../sample-files/csv/sure-start-short.csv',
  fileType = FileType.Csv
): Promise<Dataset> {
  await createTestCube(revisionId, importId);
  const testFile = path.resolve(__dirname, testFilePath);
  const testFileBuffer = fs.readFileSync(testFile);
  let mimeType = 'text/csv';
  const dataTable = new DataTable();
  dataTable.id = importId;
  dataTable.filename = `${importId.toLowerCase()}.csv`;
  dataTable.originalFilename = path.basename(testFile);
  dataTable.hash = createHash('sha256').update(testFileBuffer).digest('hex');
  dataTable.action = DataTableAction.Add;
  dataTable.fileType = fileType;
  dataTable.mimeType = mimeType;

  const fileObj: Express.Multer.File = {
    originalname: path.basename(testFile),
    mimetype: mimeType,
    path: testFile,
    fieldname: '',
    encoding: '',
    size: 0,
    stream: new Readable(),
    destination: '',
    filename: '',
    buffer: Buffer.alloc(0)
  };

  const dataTableDescriptions = await extractTableInformation(fileObj, dataTable, 'lookup_table');
  dataTableDescriptions.forEach((desc) => {
    desc.factTableColumn = desc.columnName;
  });

  switch (fileType) {
    case FileType.Csv:
      mimeType = 'text/csv';
      break;
    case FileType.Excel:
      mimeType = 'application/vnd.ms-excel';
      break;
    case FileType.Parquet:
      mimeType = 'application/vnd.apache.parquet';
      break;
    case FileType.Json:
      mimeType = 'application/json';
      break;
  }

  // First create a dataset
  const dataset = await Dataset.create({
    id: datasetId,
    createdBy: user,
    userGroupId: user?.groupRoles[0]?.groupId,
    factTable: dataTableDescriptions.map((desc) => {
      const isNoteCol = desc.columnName.toLowerCase().includes('note');
      const isDataCol = desc.columnName.toLowerCase().includes('data');
      const isMeasureCol = desc.columnName.toLowerCase().includes('measure');
      let columnType = FactTableColumnType.Dimension;
      if (isNoteCol) {
        columnType = FactTableColumnType.NoteCodes;
      } else if (isDataCol) {
        columnType = FactTableColumnType.DataValues;
      } else if (isMeasureCol) {
        columnType = FactTableColumnType.Measure;
      }
      const columnDatatype = isNoteCol ? 'VARCHAR' : desc.columnDatatype;

      return FactTableColumn.create({
        columnName: desc.columnName,
        columnIndex: desc.columnIndex,
        columnType: isDataCol ? FactTableColumnType.DataValues : columnType,
        columnDatatype
      });
    })
  }).save();

  const revision = await Revision.create({
    id: revisionId,
    datasetId,
    createdBy: user,
    revisionIndex: 1,
    metadata: ['en-GB', 'cy-GB'].map((lang) =>
      RevisionMetadata.create({
        language: lang,
        title: 'Test Dataset 1',
        summary: 'I am a small incomplete test dataset'
      })
    ),
    dataTable: DataTable.create({
      id: importId,
      filename: `${importId.toLowerCase()}.csv`,
      originalFilename: path.basename(testFile),
      hash: createHash('sha256').update(testFileBuffer).digest('hex'),
      action: DataTableAction.Add,
      fileType,
      mimeType,
      dataTableDescriptions
    })
  }).save();

  return DatasetRepository.save({
    ...dataset,
    draftRevision: revision,
    startRevision: revision,
    endRevision: revision
  });
}

const sureStartShortDimensionDescriptor = [
  {
    columnName: 'YearCode',
    dimensionType: DimensionType.DatePeriod,
    extractor: { type: 'financial', yearFormat: 'yyyyyy' },
    joinColumn: 'date_code'
  },
  {
    columnName: 'AreaCode',
    dimensionType: DimensionType.ReferenceData,
    extractor: { categories: ['Geog/ITL1', 'Geog/LA'] }
  },
  {
    columnName: 'RowRef',
    dimensionType: DimensionType.LookupTable,
    extractor: {
      sortColumn: 'sort_order',
      notesColumns: [
        { lang: 'en-gb', name: 'Notes_en' },
        { lang: 'cy-gb', name: 'Notes_cy' }
      ],
      descriptionColumns: [
        { lang: 'en-gb', name: 'Description_en' },
        { lang: 'cy-gb', name: 'Description_cy' }
      ],
      isSW2Format: true
    },
    joinColumn: 'RowRefAlt'
  }
];

const rowRefLookupTable = () => {
  const lookupTable = new LookupTable();
  lookupTable.id = crypto.randomUUID().toLowerCase();
  lookupTable.filename = 'RowRefLookupTable.csv';
  lookupTable.originalFilename = 'RowRefLookupTable.csv';
  lookupTable.fileType = FileType.Csv;
  lookupTable.isStatsWales2Format = true;
  lookupTable.mimeType = 'text/csv';
  lookupTable.hash = '89d43754ce067c9af20e06dcfa0f49297c4ed02de5a5e3c8a3a1119ecdd8f38f';
  return lookupTable;
};

async function createTestCube(revisionId: string, dataTableId: string) {
  const cubeFiles = path.resolve(__dirname, '../sample-files/test-cube');
  const quack = await duckdb();
  try {
    await linkToPostgres(quack, revisionId, true);
  } catch (err) {
    logger.error(err, 'Failed to link to postgres');
    await quack.close();
    throw err;
  }
  const createCubeSchema = `
CREATE TABLE "${revisionId}".all_notes(code VARCHAR, "language" VARCHAR, description VARCHAR);
CREATE TABLE "${revisionId}".categories(category VARCHAR PRIMARY KEY);
CREATE TABLE "${revisionId}".category_info(category VARCHAR, lang VARCHAR, description VARCHAR NOT NULL, notes VARCHAR, PRIMARY KEY(category, lang));
CREATE TABLE "${revisionId}".category_keys(category_key VARCHAR PRIMARY KEY, category VARCHAR NOT NULL);
CREATE TABLE "${revisionId}".category_key_info(category_key VARCHAR, lang VARCHAR, description VARCHAR NOT NULL, notes VARCHAR, PRIMARY KEY(category_key, lang));
CREATE TABLE "${revisionId}".default_view_cy("Gwerthoedd Data" VARCHAR, "Measure" VARCHAR, YearCode VARCHAR, "Dyddiad Cychwyn" VARCHAR, "Dyddiad Gorffen" VARCHAR, "AreaCode" VARCHAR, "RowRef" VARCHAR, "Nodiadau" VARCHAR);
CREATE TABLE "${revisionId}".default_view_en("Data Values" VARCHAR, "Measure" VARCHAR, "Year" VARCHAR, "Start Date" VARCHAR, "End Date" VARCHAR, "Local Authority" VARCHAR, "Staff Type" VARCHAR, "Notes" VARCHAR);
CREATE TABLE "${revisionId}".fact_table("YearCode" BIGINT, "AreaCode" BIGINT, "Data" DOUBLE PRECISION, "RowRef" BIGINT, "Measure" BIGINT, "NoteCodes" VARCHAR, PRIMARY KEY("YearCode", "AreaCode", "RowRef", "Measure"));
CREATE TABLE "${revisionId}".hierarchy(item_id VARCHAR, version_no INTEGER, category_key VARCHAR, parent_id VARCHAR, parent_version INTEGER, parent_category VARCHAR, PRIMARY KEY(item_id, version_no, category_key, parent_id, parent_version, parent_category));
CREATE TABLE "${revisionId}".measure(reference BIGINT, "language" VARCHAR, description VARCHAR, notes VARCHAR, sort_order INTEGER, format VARCHAR, decimals INTEGER, measure_type VARCHAR, hierarchy BIGINT);
CREATE TABLE "${revisionId}".metadata("key" VARCHAR, "value" VARCHAR);
CREATE TABLE "${revisionId}".note_codes(code VARCHAR, "language" VARCHAR, tag VARCHAR, description VARCHAR, notes VARCHAR);
CREATE TABLE "${revisionId}".raw_view_cy("Gwerthoedd Data" DOUBLE PRECISION, "Measure" VARCHAR, "YearCode" VARCHAR, "Dyddiad Cychwyn" VARCHAR, "Dyddiad Gorffen" VARCHAR, "AreaCode" VARCHAR, "RowRef" VARCHAR, "Nodiadau" VARCHAR);
CREATE TABLE "${revisionId}".raw_view_en("Data Values" DOUBLE PRECISION, "Measure" VARCHAR, "Year" VARCHAR, "Start Date" VARCHAR, "End Date" VARCHAR, "Local Authority" VARCHAR, "Staff Type" VARCHAR, "Notes" VARCHAR);
CREATE TABLE "${revisionId}".reference_data(item_id VARCHAR, version_no INTEGER, sort_order INTEGER, category_key VARCHAR, validity_start VARCHAR NOT NULL, validity_end VARCHAR, PRIMARY KEY(item_id, version_no, category_key));
CREATE TABLE "${revisionId}".reference_data_info(item_id VARCHAR, version_no INTEGER, category_key VARCHAR, lang VARCHAR, description VARCHAR NOT NULL, notes VARCHAR, PRIMARY KEY(item_id, version_no, category_key, lang));
CREATE TABLE "${revisionId}".rowref_lookup(RowRef BIGINT NOT NULL, "language" VARCHAR NOT NULL, description VARCHAR NOT NULL, notes VARCHAR, sort_order INTEGER, hierarchy BIGINT);
CREATE TABLE "${revisionId}".yearcode_lookup(YearCode BIGINT, "language" VARCHAR, description VARCHAR, hierarchy VARCHAR, date_type VARCHAR, start_date TIMESTAMP, end_date TIMESTAMP);
  `;
  await quack.exec(pgformat(`CALL postgres_execute('postgres_db', %L);`, createCubeSchema));
  const importSQL = `
COPY postgres_db.all_notes FROM '${cubeFiles}/all_notes.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.categories FROM '${cubeFiles}/categories.csv' (FORMAT 'csv', force_not_null 'category', quote '"', delimiter ',', header 1);
COPY postgres_db.category_info FROM '${cubeFiles}/category_info.csv' (FORMAT 'csv', force_not_null ('category', 'lang', 'description'), quote '"', delimiter ',', header 1);
COPY postgres_db.category_keys FROM '${cubeFiles}/category_keys.csv' (FORMAT 'csv', force_not_null ('category', 'category_key'), quote '"', delimiter ',', header 1);
COPY postgres_db.category_key_info FROM '${cubeFiles}/category_key_info.csv' (FORMAT 'csv', force_not_null ('category_key', 'lang', 'description'), quote '"', delimiter ',', header 1);
COPY postgres_db.default_view_cy FROM '${cubeFiles}/default_view_cy.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.default_view_en FROM '${cubeFiles}/default_view_en.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.fact_table FROM '${cubeFiles}/fact_table.csv' (FORMAT 'csv', force_not_null ('YearCode', 'AreaCode', 'RowRef', 'Measure'), quote '"', delimiter ',', header 1);
COPY postgres_db.hierarchy FROM '${cubeFiles}/hierarchy.csv' (FORMAT 'csv', force_not_null ('item_id', 'version_no', 'category_key', 'parent_id', 'parent_version', 'parent_category'), quote '"', delimiter ',', header 1);
COPY postgres_db.measure FROM '${cubeFiles}/measure.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.metadata FROM '${cubeFiles}/metadata.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.note_codes FROM '${cubeFiles}/note_codes.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.raw_view_cy FROM '${cubeFiles}/raw_view_cy.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.raw_view_en FROM '${cubeFiles}/raw_view_en.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY postgres_db.reference_data FROM '${cubeFiles}/reference_data.csv' (FORMAT 'csv', force_not_null ('item_id', 'version_no', 'category_key', 'validity_start'), quote '"', delimiter ',', header 1);
COPY postgres_db.reference_data_info FROM '${cubeFiles}/reference_data_info.csv' (FORMAT 'csv', force_not_null ('item_id', 'version_no', 'category_key', 'lang', 'description'), quote '"', delimiter ',', header 1);
COPY postgres_db.rowref_lookup FROM '${cubeFiles}/rowref_lookup.csv' (FORMAT 'csv', force_not_null ('RowRef', 'language', 'description'), quote '"', delimiter ',', header 1);
COPY postgres_db.yearcode_lookup FROM '${cubeFiles}/yearcode_lookup.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
  `;
  try {
    await quack.exec(importSQL);
  } catch (err) {
    logger.error(err, 'Failed to import test cube data');
    throw err;
  } finally {
    await quack.close();
  }

  const quack2 = await duckdb();
  await linkToPostgresDataTables(quack2);
  const createDataTableSQL = `
    CREATE TABLE data_tables_db."${dataTableId}"
      (
        "YearCode"  BIGINT,
        "AreaCode"  BIGINT,
        "Data"      DOUBLE PRECISION,
        "RowRef"    BIGINT,
        "Measure"   BIGINT,
        "NoteCodes" VARCHAR
      );
    `;
  try {
    await quack2.exec(createDataTableSQL);
    const loadQuery = `COPY data_tables_db."${dataTableId}" FROM '${cubeFiles}/fact_table.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);`;
    await quack2.exec(loadQuery);
  } catch (err) {
    logger.error(err, 'Failed to create test data table');
    throw err;
  } finally {
    await quack2.close();
  }
}

export async function createFullDataset(
  datasetId: string,
  revisionId: string,
  dataTableId: string,
  user: User,
  testFilePath = '../sample-files/csv/sure-start-short.csv',
  fileType = FileType.Csv,
  dimensionDescriptorJson = sureStartShortDimensionDescriptor
): Promise<void> {
  const dataset = await createSmallDataset(datasetId, revisionId, dataTableId, user, testFilePath, fileType);
  const revision = await Revision.findOneBy({ id: revisionId });

  if (!revision) {
    throw new Error('No revision found for dataset');
  }

  const factTable = await DataTable.findOneBy({ id: dataTableId });

  if (!factTable) {
    throw new Error('No import found for revision');
  }

  const dimensions = dimensionDescriptorJson.map((descriptor) => {
    return DimensionRepository.create({
      dataset,
      factTableColumn: descriptor.columnName,
      type: descriptor.dimensionType || DimensionType.Raw,
      extractor: descriptor.extractor || {},
      joinColumn: descriptor.joinColumn || null,
      metadata: [DimensionMetadata.create({ name: descriptor.columnName, language: 'en-GB' })],
      lookupTable: descriptor.dimensionType === DimensionType.LookupTable ? rowRefLookupTable() : undefined
    });
  });

  await DimensionRepository.save(dimensions);
}
