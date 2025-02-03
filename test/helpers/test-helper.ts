import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetMetadata } from '../../src/entities/dataset/dataset-metadata';
import { Revision } from '../../src/entities/dataset/revision';
import { DimensionType } from '../../src/enums/dimension-type';
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionMetadata } from '../../src/entities/dataset/dimension-metadata';
import { User } from '../../src/entities/user/user';
import { DataTable } from '../../src/entities/dataset/data-table';
import { FileType } from '../../src/enums/file-type';
import { extractTableInformation } from '../../src/services/csv-processor';
import { DataTableAction } from '../../src/enums/data-table-action';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { LookupTable } from '../../src/entities/dataset/lookup-table';
import { FactTable } from '../../src/entities/dataset/fact-table';
import { DataTableDescription } from '../../src/entities/dataset/data-table-description';

export async function createSmallDataset(
    datasetId: string,
    revisionId: string,
    importId: string,
    user: User,
    testFilePath = '../sample-files/csv/sure-start-short.csv',
    fileType = FileType.Csv
) {
    // First create a dataset
    const dataset = new Dataset();
    dataset.id = datasetId.toLowerCase();
    dataset.createdBy = user;
    dataset.live = new Date();
    await dataset.save();

    // Give it some info
    const datasetInfo = new DatasetMetadata();
    datasetInfo.dataset = dataset;
    datasetInfo.title = 'Test Dataset 1';
    datasetInfo.description = 'I am a small incomplete test dataset';
    datasetInfo.language = 'en-GB';
    await datasetInfo.save();
    dataset.metadata = [datasetInfo];

    // At the sametime we also always create a first revision
    const revision = new Revision();
    revision.id = revisionId.toLowerCase();
    revision.dataset = dataset;
    revision.createdBy = user;
    revision.revisionIndex = 1;
    await revision.save();
    dataset.revisions = [revision];

    // Attach a fact table e.g. a file to the revision
    const dataTable = new DataTable();
    dataTable.revision = revision;
    dataTable.id = importId.toLowerCase();
    dataTable.filename = `${importId.toLowerCase()}.csv`;
    const testFile = path.resolve(__dirname, testFilePath);
    dataTable.originalFilename = path.basename(testFile);
    const testFileBuffer = fs.readFileSync(testFile);
    dataTable.hash = createHash('sha256').update(testFileBuffer).digest('hex');
    dataTable.action = DataTableAction.Add;
    dataTable.fileType = fileType;
    switch (fileType) {
        case FileType.Csv:
            dataTable.mimeType = 'text/csv';
            break;
        case FileType.Excel:
            dataTable.mimeType = 'application/vnd.ms-excel';
            break;
        case FileType.Parquet:
            dataTable.mimeType = 'application/vnd.apache.parquet';
            break;
        case FileType.Json:
            dataTable.mimeType = 'application/json';
            break;
    }
    await dataTable.save();
    const factTable: FactTable[] = [];
    const factTableInfo = await extractTableInformation(testFileBuffer, fileType);
    const dataTableDescriptions = [];
    for (const info of factTableInfo) {
        const factTableCol = new FactTable();
        factTableCol.columnName = info.columnName;
        factTableCol.columnIndex = info.columnIndex;
        factTableCol.columnType = FactTableColumnType.Unknown;
        factTableCol.columnDatatype = info.columnDatatype;
        if (info.columnName.toLowerCase().indexOf('note') >= 0) {
            factTableCol.columnDatatype = 'VARCHAR';
            factTableCol.columnType = FactTableColumnType.NoteCodes;
        }
        if (info.columnName.toLowerCase().indexOf('data') >= 0) {
            factTableCol.columnType = FactTableColumnType.DataValues;
        }
        factTableCol.dataset = dataset;
        await factTableCol.save();
        factTable.push(factTableCol);
        dataTableDescriptions.push(info);
    }
    dataTable.dataTableDescriptions = dataTableDescriptions;
    await dataTable.save();
    revision.dataTable = dataTable;
    await dataset.save();
    return dataset;
}

const sureStartShortDimensionDescriptor = [
    {
        columnName: 'YearCode',
        dimensionType: DimensionType.TimePeriod,
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
    factTableId: string,
    user: User,
    testFilePath = '../sample-files/csv/sure-start-short.csv',
    fileType = FileType.Csv,
    dimensionDescriptorJson = sureStartShortDimensionDescriptor
) {
    const dataset = await createSmallDataset(datasetId, revisionId, factTableId, user, testFilePath, fileType);
    const revision = await Revision.findOneBy({ id: revisionId });
    if (!revision) {
        throw new Error('No revision found for dataset');
    }
    const factTable = await DataTable.findOneBy({ id: factTableId });
    if (!factTable) {
        throw new Error('No import found for revision');
    }
    // Create some dimensions
    dataset.dimensions = await Promise.all(
        dimensionDescriptorJson.map(async (descriptor) => {
            const dimension = new Dimension();
            dimension.dataset = dataset;
            dimension.factTableColumn = descriptor.columnName;
            dimension.type = descriptor.dimensionType || DimensionType.Raw;
            if (descriptor.dimensionType === DimensionType.LookupTable) {
                const lookupTable = rowRefLookupTable();
                const savedLookup = await lookupTable.save();
                dimension.lookupTable = savedLookup;
            }
            dimension.extractor = descriptor.extractor || {};
            dimension.joinColumn = descriptor.joinColumn || null;
            await dimension.save();
            const dimensionInfo = new DimensionMetadata();
            dimensionInfo.dimension = dimension;
            dimensionInfo.name = descriptor.columnName;
            dimensionInfo.language = 'en-GB';
            dimension.metadata = [dimensionInfo];
            await dimensionInfo.save();
            await dimension.save();
            return dimension;
        })
    );
    await dataset.save();
}
