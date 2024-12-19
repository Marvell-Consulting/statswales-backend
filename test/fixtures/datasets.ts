import { DeepPartial } from 'typeorm';

import { Dataset } from '../../src/entities/dataset/dataset';

import { publisher1 } from './users';

export const dataset1: DeepPartial<Dataset> = {
    id: '936c1ab4-2b33-4b13-8949-4316a156d24b',
    createdBy: publisher1,
    datasetInfo: [{ language: 'en-GB', title: 'Test 1 - Upload' }]
};

export const dataset2: DeepPartial<Dataset> = {
    id: 'fb440a0d-a4fb-40cb-b9e2-3f88659a5343',
    createdBy: publisher1,
    datasetInfo: [{ language: 'en-GB', title: 'Test 2 - Sources' }]
};

export const testDatasets = [
    {
        dataset: dataset1
        // csvPath: path.join(__dirname, `../csvs/dataset-1.csv`),
        // sourceTypes: [SourceType.Ignore, SourceType.Dimension, SourceType.DataValues]
    },
    {
        dataset: dataset2
        // csvPath: path.join(__dirname, `../csvs/dataset-2.csv`),
        // sourceTypes: [SourceType.Ignore, SourceType.Dimension, SourceType.DataValues, SourceType.Dimension]
    }
];
