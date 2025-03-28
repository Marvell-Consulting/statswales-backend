/* eslint-disable @typescript-eslint/no-unused-vars */
import path from 'node:path';

import { v4 as uuid } from 'uuid';
import { DeepPartial } from 'typeorm';
import { faker } from '@faker-js/faker';

import { Dataset } from '../../src/entities/dataset/dataset';

import { publisher1 } from './users';
import { Designation } from '../../src/enums/designation';
import { RevisionMetadata } from '../../src/entities/dataset/revision-metadata';

export const uploadPageTest: DeepPartial<Dataset> = {
  id: '936c1ab4-2b33-4b13-8949-4316a156d24b',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: 'en-GB', title: 'Test - Upload' },
      { language: 'cy-GB', title: 'Test - Upload' }
    ]
  }
};

export const previewPageTestA: DeepPartial<Dataset> = {
  id: 'fb440a0d-a4fb-40cb-b9e2-3f88659a5343',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: 'en-GB', title: 'Test - Preview A' },
      { language: 'en-CY', title: 'Test - Preview A' }
    ]
  }
};

export const previewPageTestB: DeepPartial<Dataset> = {
  id: '01a31d4c-fffd-4db4-b4d7-36505672df3f',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: 'en-GB', title: 'Test - Preview B' },
      { language: 'en-CY', title: 'Test - Preview B' }
    ]
  }
};

export const sourcesPageTest: DeepPartial<Dataset> = {
  id: 'cda9a27b-1b64-4922-b8b7-ef193b5f884e',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: 'en-GB', title: 'Test - Sources' },
      { language: 'en-CY', title: 'Test - Sources' }
    ]
  }
};

export const metadataTestA: DeepPartial<Dataset> = {
  id: '47dcdd38-57d4-405f-93ac-9db20bebcfc6',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: 'en-GB', title: 'Test - Metadata A' },
      { language: 'en-CY', title: 'Test - Metadata A' }
    ]
  }
};

export const metadataTestB: DeepPartial<Dataset> = {
  id: '3837564c-a901-42be-9aa6-e62232150ff6',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: 'en-GB', title: 'Test - Metadata B' },
      { language: 'en-CY', title: 'Test - Metadata B' }
    ]
  }
};

const generatePublishedDataset = (): DeepPartial<Dataset> => {
  const randDuration = `P${faker.number.int({ min: 1, max: 3 })}${faker.string.fromCharacters(['Y', 'M', 'D'])}`;

  const randMeta: Partial<RevisionMetadata> = {
    title: faker.book.title(),
    summary: faker.lorem.paragraph({ min: 1, max: 3 }),
    collection: faker.lorem.paragraph({ min: 1, max: 3 }),
    quality: faker.lorem.paragraph({ min: 1, max: 3 }),
    roundingDescription: faker.lorem.paragraph({ min: 1, max: 3 })
  };

  const live = faker.date.recent({ days: 365 });

  return {
    id: uuid(),
    createdBy: publisher1,
    publishedRevision: {
      revisionIndex: 1,
      updatedAt: live,
      roundingApplied: faker.datatype.boolean(),
      updateFrequency: faker.helpers.arrayElement(['NEVER', randDuration]),
      designation: faker.helpers.arrayElement(Object.values(Designation)),
      metadata: [
        { language: 'en-GB', ...randMeta },
        { language: 'en-CY', ...randMeta }
      ]
    },
    live
  };
};

const sureStartShort = path.join(__dirname, `../sample-files/csv/sure-start-short.csv`);

export const testDatasets = [
  { dataset: uploadPageTest },
  { dataset: previewPageTestA, csvPath: sureStartShort },
  { dataset: previewPageTestB, csvPath: sureStartShort },
  { dataset: sourcesPageTest, csvPath: sureStartShort },
  { dataset: metadataTestA, csvPath: sureStartShort },
  { dataset: metadataTestB, csvPath: sureStartShort }
  // ...Array.from({ length: 22 }, () => ({ dataset: generatePublishedDataset(), csvPath: sureStartShort }))
];
