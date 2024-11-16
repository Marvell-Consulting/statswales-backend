import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetInfo } from '../../src/entities/dataset/dataset-info';
import { Revision } from '../../src/entities/dataset/revision';
import { FileImport } from '../../src/entities/dataset/file-import';
import { CsvInfo } from '../../src/entities/dataset/csv-info';
import { Source } from '../../src/entities/dataset/source';
import { SourceType } from '../../src/enums/source-type';
import { DimensionType } from '../../src/enums/dimension-type';
import { SourceAction } from '../../src/enums/source-action';
import { DataLocation } from '../../src/enums/data-location';
import { ImportType } from '../../src/enums/import-type';
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionInfo } from '../../src/entities/dataset/dimension-info';
import { User } from '../../src/entities/user/user';

export async function createSmallDataset(datasetId: string, revisionId: string, importId: string, user: User) {
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

    // Attach an import e.g. a file to the revision
    const imp = new FileImport();
    imp.revision = revision;
    imp.id = importId.toLowerCase();
    imp.filename = `${importId.toLowerCase()}.csv`;
    const testFile = path.resolve(__dirname, `../sample-csvs/test-data-2.csv`);
    const testFileBuffer = fs.readFileSync(testFile);
    imp.hash = createHash('sha256').update(testFileBuffer).digest('hex');
    imp.type = ImportType.Draft;
    imp.mimeType = 'text/csv';
    await imp.save();
    revision.imports = [imp];

    // Its a CSV file so we need to know how to parse it
    const csvInfo = new CsvInfo();
    csvInfo.import = imp;
    csvInfo.delimiter = ',';
    csvInfo.quote = '"';
    csvInfo.linebreak = '\n';
    imp.csvInfo = [csvInfo];
    await csvInfo.save();
    imp.csvInfo = [csvInfo];

    return dataset;
}

async function createSource(
    csvField: string,
    csvIndex: number,
    action: SourceAction,
    type: SourceType,
    fileImport: FileImport,
    revision: Revision
) {
    const source = new Source();
    source.import = fileImport;
    source.revision = revision;
    source.csvField = csvField;
    source.columnIndex = csvIndex;
    source.action = action;
    source.type = type;
    await source.save();

    return source;
}

async function createDimension(
    csvField: string,
    description: string,
    dataset: Dataset,
    revision: Revision,
    source: Source
) {
    const dimension = new Dimension();
    dimension.dataset = dataset;
    dimension.startRevision = revision;
    dimension.type = DimensionType.Raw;
    await dimension.save();

    const dimensionInfo = new DimensionInfo();
    dimensionInfo.dimension = dimension;
    dimensionInfo.name = csvField;
    dimensionInfo.description = description;
    dimensionInfo.language = 'en-GB';
    await dimensionInfo.save();
    dimension.dimensionInfo = [dimensionInfo];
    await dimension.save();

    dimension.sources = [source];
    source.dimension = dimension;
    await source.save();

    return dimension;
}

export async function createFullDataset(datasetId: string, revisionId: string, importId: string, user: User) {
    const dataset = await createSmallDataset(datasetId, revisionId, importId, user);
    const revision = await Revision.findOneBy({ id: revisionId });
    if (!revision) {
        throw new Error('No revision found for dataset');
    }
    const imp = await FileImport.findOneBy({ id: importId });
    if (!imp) {
        throw new Error('No import found for revision');
    }
    const sourceDescriptions = [
        { csvField: 'ID', description: 'unique identifier', action: SourceAction.Create, type: SourceType.Ignore },
        { csvField: 'Text', description: 'Some test', action: SourceAction.Create, type: SourceType.Dimension },
        {
            csvField: 'Number',
            description: 'some data values',
            action: SourceAction.Create,
            type: SourceType.DataValues
        },
        { csvField: 'Date', description: 'some dimensions', action: SourceAction.Create, type: SourceType.Dimension }
    ];
    // Create some sources for each of the columns in the CSV
    const sources: Source[] = [];
    for (let i = 0; i < sourceDescriptions.length; i++) {
        const source = await createSource(
            sourceDescriptions[i].csvField,
            i,
            sourceDescriptions[i].action,
            sourceDescriptions[i].type,
            imp,
            revision
        );
        sources.push(source);
    }
    imp.sources = sources;
    await imp.save();

    // // Next create some dimensions
    const dimensions: Dimension[] = [];
    for (let i = 0; i < sourceDescriptions.length; i++) {
        const dimesnion = await createDimension(
            sourceDescriptions[i].csvField,
            sourceDescriptions[i].description,
            dataset,
            revision,
            sources[i]
        );
        dimensions.push(dimesnion);
    }
    dataset.dimensions = dimensions;
}
