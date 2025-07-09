import path from 'node:path';
import fs from 'node:fs';

import { DeepPartial } from 'typeorm';

import { Dataset } from '../../../entities/dataset/dataset';

import { publisher1 } from './users';
import { group1 } from './group';
import { Locale } from '../../../enums/locale';

export const uploadPageTest: DeepPartial<Dataset> = {
  id: '936c1ab4-2b33-4b13-8949-4316a156d24b',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Upload' },
      { language: Locale.WelshGb, title: 'Test - Upload' }
    ]
  },
  userGroupId: group1.id
};

export const previewPageTestA: DeepPartial<Dataset> = {
  id: 'fb440a0d-a4fb-40cb-b9e2-3f88659a5343',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Preview A' },
      { language: Locale.WelshGb, title: 'Test - Preview A' }
    ]
  },
  userGroupId: group1.id
};

export const previewPageTestB: DeepPartial<Dataset> = {
  id: '01a31d4c-fffd-4db4-b4d7-36505672df3f',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Preview B' },
      { language: Locale.WelshGb, title: 'Test - Preview B' }
    ]
  },
  userGroupId: group1.id
};

export const sourcesPageTest: DeepPartial<Dataset> = {
  id: 'cda9a27b-1b64-4922-b8b7-ef193b5f884e',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Sources' },
      { language: Locale.WelshGb, title: 'Test - Sources' }
    ]
  },
  userGroupId: group1.id
};

export const metadataTestA: DeepPartial<Dataset> = {
  id: '47dcdd38-57d4-405f-93ac-9db20bebcfc6',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Metadata A' },
      { language: Locale.WelshGb, title: 'Test - Metadata A' }
    ]
  },
  userGroupId: group1.id
};

export const metadataTestB: DeepPartial<Dataset> = {
  id: '3837564c-a901-42be-9aa6-e62232150ff6',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Metadata B' },
      { language: Locale.WelshGb, title: 'Test - Metadata B' }
    ]
  },
  userGroupId: group1.id
};

export const moveGroupTest: DeepPartial<Dataset> = {
  id: '7f247810-08c3-4cb2-acca-b2c77dc03770',
  createdBy: publisher1,
  draftRevision: {
    revisionIndex: 1,
    metadata: [
      { language: Locale.EnglishGb, title: 'Test - Move group' },
      { language: Locale.WelshGb, title: 'Test - Move group' }
    ]
  },
  userGroupId: group1.id
};

const csvData = `YearCode,AreaCode,Data,RowRef,Measure,NoteCodes
202223,512,1.442546584,2,2,
202223,512,1.563664596,3,2,
202223,512,3.220496894,1,2,
202223,512,929.0,2,1,
202223,512,1007.0,3,1,
202223,512,2074.0,1,1,
202223,596,0.93745237,3,2,a
202223,596,1.635044737,2,2,a
202223,596,3.53077987,1,2,a
202223,596,33213.0,3,1,t
202223,596,57928.0,2,1,t
202223,596,125092.0,1,1,t
202122,512,1.190839695,3,2,
202122,512,1.253435115,2,2,
202122,512,3.458015267,1,2,
202122,512,780.0,3,1,
202122,512,821.0,2,1,
202122,512,2265.0,1,1,
202122,596,1.060637144,3,2,a
202122,596,1.507751824,2,2,a
202122,596,4.030567686,1,2,a
202122,596,36190.0,3,1,t
202122,596,51446.0,2,1,t
202122,596,137527.0,1,1,t
`;

const tmpFilePath = path.join(__dirname, `./sure-start-short.csv`);

export const setupTmpCsv = (): void => {
  fs.writeFileSync(tmpFilePath, csvData);
};

export const testDatasets = [
  { dataset: uploadPageTest },
  { dataset: previewPageTestA, csvPath: tmpFilePath },
  { dataset: previewPageTestB, csvPath: tmpFilePath },
  { dataset: sourcesPageTest, csvPath: tmpFilePath },
  { dataset: metadataTestA, csvPath: tmpFilePath },
  { dataset: metadataTestB, csvPath: tmpFilePath },
  { dataset: moveGroupTest, csvPath: tmpFilePath }
];
