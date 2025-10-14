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
import { logger } from '../../src/utils/logger';
import { Readable } from 'node:stream';
import { createAllCubeFiles } from '../../src/services/cube-builder';
import { parse } from 'csv';
import { cubeDataSource } from '../../src/db/cube-source';
import { uuidV4 } from '../../src/utils/uuid';

export async function createSmallDataset(
  datasetId: string,
  revisionId: string,
  importId: string,
  user: User,
  testFilePath = '../sample-files/csv/sure-start-data.csv',
  fileType = FileType.Csv
): Promise<Dataset> {
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

  const dataTableDescriptions = await extractTableInformation(fileObj, dataTable, 'data_table');
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
      const columnDatatype = isNoteCol ? 'VARCHAR' : desc.columnDatatype;

      return FactTableColumn.create({
        columnName: desc.columnName,
        columnIndex: desc.columnIndex,
        columnType: FactTableColumnType.Unknown,
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

  const savedDataset = await DatasetRepository.save({
    ...dataset,
    draftRevision: revision,
    startRevision: revision,
    endRevision: revision
  });

  try {
    await createTestCube(savedDataset.draftRevision.id, savedDataset.draftRevision.dataTable!.id);
  } catch (err) {
    logger.error(err);
  }

  try {
    await createAllCubeFiles(savedDataset.id, revision.id);
  } catch (error) {
    logger.error(error);
  }

  return savedDataset;
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
    dimensionType: DimensionType.Text
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
  lookupTable.id = uuidV4();
  lookupTable.filename = 'RowRefLookupTable.csv';
  lookupTable.originalFilename = 'RowRefLookupTable.csv';
  lookupTable.fileType = FileType.Csv;
  lookupTable.isStatsWales2Format = true;
  lookupTable.mimeType = 'text/csv';
  lookupTable.hash = '89d43754ce067c9af20e06dcfa0f49297c4ed02de5a5e3c8a3a1119ecdd8f38f';
  return lookupTable;
};

async function createTestCube(revisionId: string, dataTableId: string) {
  const cubeDB = await cubeDataSource.createQueryRunner();

  try {
    await cubeDB.query(pgformat('CREATE SCHEMA IF NOT EXISTS %I;', revisionId));
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revisionId));
    const createDataTableSQL = `
      CREATE TABLE data_tables."${dataTableId}"
        (
          "YearCode"  BIGINT,
          "AreaCode"  BIGINT,
          "Data"      DOUBLE PRECISION,
          "RowRef"    BIGINT,
          "Measure"   BIGINT,
          "NoteCodes" VARCHAR
        );
    `;
    await cubeDB.query(createDataTableSQL);
    const parserOpts = { delimiter: ',', bom: true, skip_empty_lines: true, columns: true };
    const dataFile = path.resolve(__dirname, '../sample-files/csv/sure-start-data.csv');
    const parseCSV = async (): Promise<void> => {
      const csvParser: AsyncIterable<any> = fs.createReadStream(dataFile).pipe(parse(parserOpts));
      for await (const row of csvParser) {
        await cubeDB.query(pgformat('INSERT INTO data_tables.%I VALUES (%L);', dataTableId, Object.values(row)));
      }
    };
    await parseCSV();
  } catch (err) {
    logger.error(err, 'Failed to create test data table');
    throw err;
  } finally {
    await cubeDB.release();
  }
}

export async function createFullDataset(
  datasetId: string,
  revisionId: string,
  dataTableId: string,
  user: User,
  testFilePath = '../sample-files/csv/sure-start-data.csv',
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
