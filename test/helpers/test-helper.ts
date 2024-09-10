import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import { Dataset } from '../../src/entities/dataset';
import { DatasetInfo } from '../../src/entities/dataset_info';
import { Revision } from '../../src/entities/revision';
import { FileImport } from '../../src/entities/import_file';
import { CsvInfo } from '../../src/entities/csv_info';
import { Source } from '../../src/entities/source';
import { Dimension } from '../../src/entities/dimension';
import { DimensionType } from '../../src/entities/dimension_type';
import { DimensionInfo } from '../../src/entities/dimension_info';
import { User } from '../../src/entities/user';

export async function createFullDataset(datasetId: string, revisionId: string, importId: string, dimensionId: string) {
    const user = User.getTestUser();
    await user.save();
    // First create a dataset
    const dataset = new Dataset();
    dataset.id = datasetId;
    dataset.createdBy = Promise.resolve(user);
    dataset.live = new Date(Date.now());
    // Give it some info
    const datasetInfo = new DatasetInfo();
    datasetInfo.dataset = Promise.resolve(dataset);
    datasetInfo.title = 'Test Dataset 1';
    datasetInfo.description = 'I am the first test dataset';
    datasetInfo.language = 'en-GB';
    dataset.datasetInfo = Promise.resolve([datasetInfo]);
    // At the sametime we also always create a first revision
    const revision = new Revision();
    revision.id = revisionId;
    revision.dataset = Promise.resolve(dataset);
    revision.createdBy = Promise.resolve(user);
    revision.revisionIndex = 1;
    dataset.revisions = Promise.resolve([revision]);
    // Attach an import e.g. a file to the revision
    const imp = new FileImport();
    imp.revision = Promise.resolve(revision);
    imp.id = importId;
    imp.filename = 'FA07BE9D-3495-432D-8C1F-D0FC6DAAE359.csv';
    const testFile1 = path.resolve(__dirname, `../sample-csvs/test-data-2.csv`);
    const testFile2Buffer = fs.readFileSync(testFile1);
    imp.hash = createHash('sha256').update(testFile2Buffer).digest('hex');
    // First is a draft import and a first upload so everything is in blob storage
    imp.location = 'BlobStorage';
    imp.type = 'Draft';
    imp.mime_type = 'text/csv';
    // Its a CSV file so we need to know how to parse it
    const csvInfo = new CsvInfo();
    csvInfo.import = Promise.resolve(imp);
    csvInfo.delimiter = ',';
    csvInfo.quote = '"';
    csvInfo.linebreak = '\n';
    imp.csvInfo = Promise.resolve([csvInfo]);
    revision.imports = Promise.resolve([imp]);
    await dataset.save();
    // Create some sources for each of the columns in the CSV
    const sources: Source[] = [];
    const source1 = new Source();
    source1.id = '304574E6-8DD0-4654-BE67-FA055C9F7C81';
    source1.import = Promise.resolve(imp);
    source1.revision = Promise.resolve(revision);
    source1.csvField = 'ID';
    source1.columnIndex = 0;
    source1.action = 'ignore';
    sources.push(source1);
    const source2 = new Source();
    source2.id = 'D3D3D3D3-8DD0-4654-BE67-FA055C9F7C81';
    source2.import = Promise.resolve(imp);
    source2.revision = Promise.resolve(revision);
    source2.csvField = 'Text';
    source2.columnIndex = 1;
    source2.action = 'create';
    sources.push(source2);
    const source3 = new Source();
    source3.id = 'D62FA390-9AB2-496E-A6CA-0C0E2FCF206E';
    source3.import = Promise.resolve(imp);
    source3.revision = Promise.resolve(revision);
    source3.csvField = 'Number';
    source3.columnIndex = 2;
    source3.action = 'create';
    sources.push(source3);
    const source4 = new Source();
    source4.id = 'FB25D668-54F2-44EF-99FE-B4EDC4AF2911';
    source4.import = Promise.resolve(imp);
    source4.revision = Promise.resolve(revision);
    source4.csvField = 'Date';
    source4.columnIndex = 3;
    source4.action = 'create';
    sources.push(source4);
    imp.sources = Promise.resolve(sources);
    await imp.save();
    // Next create some dimensions
    const dimensions: Dimension[] = [];
    const dimension1 = new Dimension();
    dimension1.id = dimensionId;
    dimension1.dataset = Promise.resolve(dataset);
    dimension1.startRevision = Promise.resolve(revision);
    dimension1.type = DimensionType.RAW;
    const dimension1Info = new DimensionInfo();
    dimension1Info.dimension = Promise.resolve(dimension1);
    dimension1Info.name = 'ID';
    dimension1Info.description = 'Unique identifier';
    dimension1Info.language = 'en-GB';
    dimension1.dimensionInfo = Promise.resolve([dimension1Info]);
    dimension1.sources = Promise.resolve([source1]);
    source1.dimension = Promise.resolve(dimension1);
    dimensions.push(dimension1);
    // Dimension 2
    const dimension2 = new Dimension();
    dimension2.id = '61D51F82-0771-4C90-849E-55FFA7A4D802';
    dimension2.dataset = Promise.resolve(dataset);
    dimension2.startRevision = Promise.resolve(revision);
    dimension2.type = DimensionType.TEXT;
    const dimension2Info = new DimensionInfo();
    dimension2Info.dimension = Promise.resolve(dimension2);
    dimension2Info.name = 'Text';
    dimension2Info.description = 'Sample text strings';
    dimension2Info.language = 'en-GB';
    dimension2.dimensionInfo = Promise.resolve([dimension2Info]);
    dimension2.sources = Promise.resolve([source2]);
    source2.dimension = Promise.resolve(dimension2);
    dimensions.push(dimension2);
    // Dimension 3
    const dimension3 = new Dimension();
    dimension3.id = 'F4D5B0F4-180E-4020-AAD5-9300B673D92B';
    dimension3.dataset = Promise.resolve(dataset);
    dimension3.startRevision = Promise.resolve(revision);
    dimension3.type = DimensionType.NUMERIC;
    const dimension3Info = new DimensionInfo();
    dimension3Info.dimension = Promise.resolve(dimension3);
    dimension3Info.name = 'Value';
    dimension3Info.description = 'Sample numeric values';
    dimension3Info.language = 'en-GB';
    dimension3.dimensionInfo = Promise.resolve([dimension3Info]);
    dimension3.sources = Promise.resolve([source3]);
    source3.dimension = Promise.resolve(dimension3);
    dimensions.push(dimension3);
    // Dimension 4
    const dimension4 = new Dimension();
    dimension4.id = 'C24962F4-F395-40EF-B4DD-270E90E10972';
    dimension4.dataset = Promise.resolve(dataset);
    dimension4.startRevision = Promise.resolve(revision);
    dimension4.type = DimensionType.TIME_POINT;
    const dimension4Info = new DimensionInfo();
    dimension4Info.dimension = Promise.resolve(dimension4);
    dimension4Info.name = 'Date';
    dimension4Info.description = 'Sample date values';
    dimension4Info.language = 'en-GB';
    dimension4.dimensionInfo = Promise.resolve([dimension4Info]);
    dimension4.sources = Promise.resolve([source4]);
    source4.dimension = Promise.resolve(dimension4);
    dimensions.push(dimension4);
    dataset.dimensions = Promise.resolve(dimensions);
    await dataset.save();
}

export async function createSmallDataset(datasetId: string, revisionId: string, importId: string) {
    const user = User.getTestUser();
    await user.save();
    // First create a dataset
    const dataset = new Dataset();
    dataset.id = datasetId;
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
    revision.id = revisionId;
    revision.dataset = Promise.resolve(dataset);
    revision.createdBy = Promise.resolve(user);
    revision.revisionIndex = 1;
    dataset.revisions = Promise.resolve([revision]);
    // Attach an import e.g. a file to the revision
    const imp = new FileImport();
    imp.revision = Promise.resolve(revision);
    imp.id = importId;
    imp.filename = 'FA07BE9D-3495-432D-8C1F-D0FC6DAAE359.csv';
    const testFile1 = path.resolve(__dirname, `../sample-csvs/test-data-2.csv`);
    const testFile2Buffer = fs.readFileSync(testFile1);
    imp.hash = createHash('sha256').update(testFile2Buffer).digest('hex');
    // First is a draft import and a first upload so everything is in blob storage
    imp.location = 'BlobStorage';
    imp.type = 'Draft';
    imp.mime_type = 'text/csv';
    // Its a CSV file so we need to know how to parse it
    const csvInfo = new CsvInfo();
    csvInfo.import = Promise.resolve(imp);
    csvInfo.delimiter = ',';
    csvInfo.quote = '"';
    csvInfo.linebreak = '\n';
    imp.csvInfo = Promise.resolve([csvInfo]);
    revision.imports = Promise.resolve([imp]);
    await dataset.save();
    return dataset;
}
