import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

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

export async function createSmallDataset(
  datasetId: string,
  revisionId: string,
  importId: string,
  user: User,
  testFilePath = '../sample-files/csv/sure-start-short.csv',
  fileType = FileType.Csv
): Promise<Dataset> {
  const testFile = path.resolve(__dirname, testFilePath);
  const testFileBuffer = fs.readFileSync(testFile);
  let mimeType = 'text/csv';

  const dataTableDescriptions = await extractTableInformation(testFileBuffer, fileType);

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
    factTable: dataTableDescriptions.map((desc) => {
      const isNoteCol = desc.columnName.toLowerCase().includes('note');
      const isDataCol = desc.columnName.toLowerCase().includes('data');
      const columnType = isNoteCol ? FactTableColumnType.NoteCodes : FactTableColumnType.Unknown;
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
        { lang: 'en', name: 'Notes_en' },
        { lang: 'cy', name: 'Notes_cy' }
      ],
      descriptionColumns: [
        { lang: 'en', name: 'Description_en' },
        { lang: 'cy', name: 'Description_cy' }
      ]
    },
    joinColumn: 'RowRefAlt'
  }
];

const rowRefLookupTable = () => {
  const lookupTable = new LookupTable();
  lookupTable.id = crypto.randomUUID().toLowerCase();
  lookupTable.filename = 'RowRefLookupTable.csv';
  lookupTable.fileType = FileType.Csv;
  lookupTable.isStatsWales2Format = true;
  lookupTable.mimeType = 'text/csv';
  lookupTable.hash = '89d43754ce067c9af20e06dcfa0f49297c4ed02de5a5e3c8a3a1119ecdd8f38f';
  return lookupTable;
};

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
