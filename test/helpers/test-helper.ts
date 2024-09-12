import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import { Dataset } from '../../src/entities/dataset';
import { DatasetInfo } from '../../src/entities/dataset-info';
import { Revision } from '../../src/entities/revision';
import { FileImport } from '../../src/entities/file-import';
import { CsvInfo } from '../../src/entities/csv-info';
import { Source } from '../../src/entities/source';
import { SourceType } from '../../src/enums/source-type';
import { Dimension } from '../../src/entities/dimension';
import { DimensionType } from '../../src/enums/dimension-type';
import { DimensionInfo } from '../../src/entities/dimension-info';
import { User } from '../../src/entities/user';
import { SourceAction } from '../../src/enums/source-action';

export async function createSmallDataset(datasetId: string, revisionId: string, importId: string, user: User) {
    // First create a dataset
    const dataset = new Dataset();
    dataset.id = datasetId.toLowerCase();
    dataset.createdBy = Promise.resolve(user);
    dataset.live = new Date(Date.now());

    // Give it some info
    const datasetInfo = new DatasetInfo();
    datasetInfo.dataset = Promise.resolve(dataset);
    datasetInfo.title = 'Test Dataset 1';
    datasetInfo.description = 'I am a small incomplete test dataset';
    datasetInfo.language = 'en-GB';
    dataset.datasetInfo = Promise.resolve([datasetInfo]);

    // At the sametime we also always create a first revision
    const revision = new Revision();
    revision.id = revisionId.toLowerCase();
    revision.dataset = Promise.resolve(dataset);
    revision.createdBy = Promise.resolve(user);
    revision.revisionIndex = 1;
    dataset.revisions = Promise.resolve([revision]);

    // Attach an import e.g. a file to the revision
    const imp = new FileImport();
    imp.revision = Promise.resolve(revision);
    imp.id = importId.toLowerCase();
    imp.filename = `${importId.toLowerCase()}.csv`;
    const testFile1 = path.resolve(__dirname, `../sample-csvs/test-data-2.csv`);
    const testFile2Buffer = fs.readFileSync(testFile1);
    imp.hash = createHash('sha256').update(testFile2Buffer).digest('hex');

    // First is a draft import and a first upload so everything is in blob storage
    imp.location = 'BlobStorage';
    imp.type = 'Draft';
    imp.mimeType = 'text/csv';

    // Its a CSV file so we need to know how to parse it
    const csvInfo = new CsvInfo();
    csvInfo.import = Promise.resolve(imp);
    csvInfo.delimiter = ',';
    csvInfo.quote = '"';
    csvInfo.linebreak = '\n';
    imp.csvInfo = Promise.resolve([csvInfo]);
    revision.imports = Promise.resolve([imp]);
    // Save and return the result
    await dataset.save();
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
    source.id = crypto.randomUUID().toLowerCase();
    source.import = Promise.resolve(fileImport);
    source.revision = Promise.resolve(revision);
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
    dimension.id = crypto.randomUUID().toLowerCase();
    dimension.dataset = Promise.resolve(dataset);
    dimension.startRevision = Promise.resolve(revision);
    dimension.type = DimensionType.RAW;
    const dimensionInfo = new DimensionInfo();
    dimensionInfo.dimension = Promise.resolve(dimension);
    dimensionInfo.name = csvField;
    dimensionInfo.description = description;
    dimensionInfo.language = 'en-GB';
    dimension.dimensionInfo = Promise.resolve([dimensionInfo]);
    await dimension.save();
    dimension.sources = Promise.resolve([source]);
    source.dimension = Promise.resolve(dimension);
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
        { csvField: 'ID', description: 'unique identifier', action: SourceAction.CREATE, type: SourceType.IGNORE },
        { csvField: 'Text', description: 'Some test', action: SourceAction.CREATE, type: SourceType.DIMENSION },
        {
            csvField: 'Number',
            description: 'some data values',
            action: SourceAction.CREATE,
            type: SourceType.DATAVALUES
        },
        { csvField: 'Date', description: 'some dimensions', action: SourceAction.CREATE, type: SourceType.DIMENSION }
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
    imp.sources = Promise.resolve(sources);
    await imp.save();

    // // Next create some dimensions
    const dimensions: Dimension[] = [];
    for(let i = 0; i < sourceDescriptions.length; i++) {
        const dimesnion = await createDimension(
            sourceDescriptions[i].csvField,
            sourceDescriptions[i].description,
            dataset,
            revision,
            sources[i]
        );
        dimensions.push(dimesnion);
    }
    dataset.dimensions = Promise.resolve(dimensions);
}
