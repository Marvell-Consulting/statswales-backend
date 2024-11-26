import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetInfo } from '../../src/entities/dataset/dataset-info';
import { Revision } from '../../src/entities/dataset/revision';
import { SourceType } from '../../src/enums/source-type';
import { DimensionType } from '../../src/enums/dimension-type';
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionInfo } from '../../src/entities/dataset/dimension-info';
import { User } from '../../src/entities/user/user';
import { FactTable } from '../../src/entities/dataset/fact-table';
import { Filetype } from '../../src/enums/filetype';
import { FactTableInfo } from '../../src/entities/dataset/fact-table-info';
import { extractTableInformation } from '../../src/controllers/csv-processor';

export async function createSmallDataset(
    datasetId: string,
    revisionId: string,
    importId: string,
    user: User,
    testFilePath = '../sample-files/csv/sure-start-short.csv',
    fileType = Filetype.Csv
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
    const testFileBuffer = fs.readFileSync(testFile);
    factTable.hash = createHash('sha256').update(testFileBuffer).digest('hex');
    factTable.fileType = fileType;
    switch (fileType) {
        case Filetype.Csv:
            factTable.linebreak = '\n';
            factTable.delimiter = ',';
            factTable.quote = '"';
            factTable.mimeType = 'text/csv';
            break;
        case Filetype.Excel:
            factTable.mimeType = 'application/vnd.ms-excel';
            break;
        case Filetype.Parquet:
            factTable.mimeType = 'application/vnd.apache.parquet';
            break;
        case Filetype.Json:
            factTable.mimeType = 'application/json';
            break;
    }
    await factTable.save();
    factTable.factTableInfo = await extractTableInformation(testFileBuffer, fileType);
    await factTable.save();
    revision.factTables = [factTable];
    await dataset.save();
    return dataset;
}

const sureStartShortDimensionDescriptor = [
    {
        columnName: 'YearCode',
        dimensionType: DimensionType.TimePeriod,
        extractor: { type: 'financial', yearFormat: 'yyyyyy' }
    },
    {
        columnName: 'AreaCode',
        dimensionType: DimensionType.ReferenceData
    },
    {
        columnName: 'RowRef',
        dimensionType: DimensionType.LookupTable
    }
];

export async function createFullDataset(
    datasetId: string,
    revisionId: string,
    factTableId: string,
    user: User,
    testFilePath = '../sample-files/csv/sure-start-short.csv',
    fileType = Filetype.Csv,
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
            dimension.type = DimensionType.Raw;
            dimension.factTableColumn = descriptor.columnName;
            dimension.type = descriptor.dimensionType || DimensionType.Raw;
            dimension.extractor = descriptor.extractor || {};
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
