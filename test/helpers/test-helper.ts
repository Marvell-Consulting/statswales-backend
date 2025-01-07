import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetInfo } from '../../src/entities/dataset/dataset-info';
import { Revision } from '../../src/entities/dataset/revision';
import { DimensionType } from '../../src/enums/dimension-type';
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionInfo } from '../../src/entities/dataset/dimension-info';
import { User } from '../../src/entities/user/user';
import { FactTable } from '../../src/entities/dataset/fact-table';
import { FileType } from '../../src/enums/file-type';
import { extractTableInformation } from '../../src/services/csv-processor';
import { FactTableAction } from '../../src/enums/fact-table-action';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { LookupTable } from '../../src/entities/dataset/lookup-table';

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
    const datasetInfo = new DatasetInfo();
    datasetInfo.dataset = dataset;
    datasetInfo.title = 'Test Dataset 1';
    datasetInfo.description = 'I am a small incomplete test dataset';
    datasetInfo.language = 'en-GB';
    await datasetInfo.save();
    dataset.datasetInfo = [datasetInfo];

    // At the sametime we also always create a first revision
    const revision = new Revision();
    revision.id = revisionId.toLowerCase();
    revision.dataset = dataset;
    revision.createdBy = user;
    revision.revisionIndex = 1;
    await revision.save();
    dataset.revisions = [revision];

    // Attach a fact table e.g. a file to the revision
    const factTable = new FactTable();
    factTable.revision = revision;
    factTable.id = importId.toLowerCase();
    factTable.filename = `${importId.toLowerCase()}.csv`;
    const testFile = path.resolve(__dirname, testFilePath);
    factTable.originalFilename = path.basename(testFile);
    const testFileBuffer = fs.readFileSync(testFile);
    factTable.hash = createHash('sha256').update(testFileBuffer).digest('hex');
    factTable.action = FactTableAction.Add;
    factTable.fileType = fileType;
    switch (fileType) {
        case FileType.Csv:
            factTable.linebreak = '\n';
            factTable.delimiter = ',';
            factTable.quote = '"';
            factTable.mimeType = 'text/csv';
            break;
        case FileType.Excel:
            factTable.mimeType = 'application/vnd.ms-excel';
            break;
        case FileType.Parquet:
            factTable.mimeType = 'application/vnd.apache.parquet';
            break;
        case FileType.Json:
            factTable.mimeType = 'application/json';
            break;
    }
    await factTable.save();
    const factTableInfo = await extractTableInformation(testFileBuffer, fileType);
    factTable.factTableInfo = factTableInfo.map((info) => {
        if (info.columnName.toLowerCase().indexOf('note') >= 0) {
            info.columnType = FactTableColumnType.NoteCodes;
        }
        if (info.columnName.toLowerCase().indexOf('data') >= 0) {
            info.columnType = FactTableColumnType.DataValues;
        }
        return info;
    });
    await factTable.save();
    revision.factTables = [factTable];
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
    lookupTable.linebreak = '\n';
    lookupTable.delimiter = ',';
    lookupTable.quote = '"';
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
    const factTable = await FactTable.findOneBy({ id: factTableId });
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
            const dimensionInfo = new DimensionInfo();
            dimensionInfo.dimension = dimension;
            dimensionInfo.name = descriptor.columnName;
            dimensionInfo.language = 'en-GB';
            dimension.dimensionInfo = [dimensionInfo];
            await dimensionInfo.save();
            await dimension.save();
            return dimension;
        })
    );
    await dataset.save();
}
