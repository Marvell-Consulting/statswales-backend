import path from 'node:path';

import { DeepPartial } from 'typeorm';

import { Dataset } from '../../src/entities/dataset/dataset';

import { publisher1 } from './users';

export const uploadPageTest: DeepPartial<Dataset> = {
    id: '936c1ab4-2b33-4b13-8949-4316a156d24b',
    createdBy: publisher1,
    datasetInfo: [
        { language: 'en-GB', title: 'Test - Upload' },
        { language: 'cy-GB', title: 'Test - Upload' }
    ]
};

export const previewPageTestA: DeepPartial<Dataset> = {
    id: 'fb440a0d-a4fb-40cb-b9e2-3f88659a5343',
    createdBy: publisher1,
    datasetInfo: [
        { language: 'en-GB', title: 'Test - Preview A' },
        { language: 'en-CY', title: 'Test - Preview A' }
    ]
};

export const previewPageTestB: DeepPartial<Dataset> = {
    id: '01a31d4c-fffd-4db4-b4d7-36505672df3f',
    createdBy: publisher1,
    datasetInfo: [
        { language: 'en-GB', title: 'Test - Preview B' },
        { language: 'en-CY', title: 'Test - Preview B' }
    ]
};

export const sourcesPageTest: DeepPartial<Dataset> = {
    id: 'cda9a27b-1b64-4922-b8b7-ef193b5f884e',
    createdBy: publisher1,
    datasetInfo: [
        { language: 'en-GB', title: 'Test - Sources' },
        { language: 'en-CY', title: 'Test - Sources' }
    ]
};

export const metadataTest: DeepPartial<Dataset> = {
    id: '47dcdd38-57d4-405f-93ac-9db20bebcfc6',
    createdBy: publisher1,
    datasetInfo: [
        { language: 'en-GB', title: 'Test - Metadata' },
        { language: 'en-CY', title: 'Test - Metadata' }
    ]
};

export const testDatasets = [
    { dataset: uploadPageTest },
    { dataset: previewPageTestA, csvPath: path.join(__dirname, `../sample-files/csv/cheeses.csv`) },
    { dataset: previewPageTestB, csvPath: path.join(__dirname, `../sample-files/csv/cheeses.csv`) },
    { dataset: sourcesPageTest, csvPath: path.join(__dirname, `../sample-files/csv/sure-start-short.csv`) },
    { dataset: metadataTest, csvPath: path.join(__dirname, `../sample-files/csv/sure-start-short.csv`) }
];
