/* eslint-disable no-console */
import { readFile } from 'node:fs/promises';
import path from 'node:path';

import { DataSource, DeepPartial } from 'typeorm';
import { omit } from 'lodash';
import { Seeder } from '@jorgebodega/typeorm-seeding';

import { User } from '../../src/entities/user/user';
import { Dataset } from '../../src/entities/dataset/dataset';
import { validateAndUpload } from '../../src/services/csv-processor';
import { Revision } from '../../src/entities/dataset/revision';
import { GroupRole } from '../enums/group-role';
import { Designation } from '../enums/designation';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { convertDataTableToLookupTable } from '../utils/lookup-table-utils';
import { Provider } from '../entities/dataset/provider';
import { getFileService } from '../utils/get-file-service';
import { TempFile } from '../interfaces/temp-file';

const user: DeepPartial<User> = {
  id: 'fceaeab9-d515-4f90-ba25-38ffb3dab3b9',
  provider: 'entraid',
  providerUserId: 'seed_publisher_1',
  givenName: 'Seed',
  familyName: 'Publisher',
  email: 'seed.publisher@example.com',
  groupRoles: [
    {
      id: 'c1e577b0-2c1c-4af9-89ad-8a9e44912cea',
      groupId: '24bf9f9c-898a-4d23-ae1e-35a6ff30ee63',
      roles: [GroupRole.Editor]
    }
  ]
};

const approvedDataset: DeepPartial<Dataset> = {
  id: 'f12bed26-18ac-4cb9-bcdb-24ed155f29a1',
  createdBy: user,
  createdAt: '2025-05-01 13:10:40.176625+00',
  live: '2025-05-01 13:20:00+00',
  startDate: '2013-04-01',
  endDate: '2024-03-31',
  factTable: [
    {
      columnName: 'Data',
      columnType: FactTableColumnType.DataValues,
      columnDatatype: 'DOUBLE',
      columnIndex: 2
    },
    {
      columnName: 'NoteCodes',
      columnType: FactTableColumnType.NoteCodes,
      columnDatatype: 'VARCHAR',
      columnIndex: 5
    },
    {
      columnName: 'Measure',
      columnType: FactTableColumnType.Measure,
      columnDatatype: 'BIGINT',
      columnIndex: 4
    },
    {
      columnName: 'YearCode',
      columnType: FactTableColumnType.Dimension,
      columnDatatype: 'BIGINT',
      columnIndex: 0
    },
    {
      columnName: 'AreaCode',
      columnType: FactTableColumnType.Dimension,
      columnDatatype: 'BIGINT',
      columnIndex: 1
    },
    {
      columnName: 'RowRef',
      columnType: FactTableColumnType.Dimension,
      columnDatatype: 'BIGINT',
      columnIndex: 3
    }
  ],
  measure: {
    id: '84cfac80-0b83-4b37-b759-295fd61d4ce5',
    factTableColumn: 'Measure',
    joinColumn: 'Reference Code',
    extractor: {
      sortColumn: 'Sort',
      isSW2Format: true,
      notesColumns: [
        { lang: 'en-gb', name: 'Notes English' },
        { lang: 'cy-gb', name: 'Notes Welsh' }
      ],
      decimalColumn: 'Decimals',
      tableLanguage: 'en-GB',
      descriptionColumns: [
        { lang: 'en-gb', name: 'Description English' },
        { lang: 'cy-gb', name: 'Description Welsh' }
      ]
    }
  },
  dimensions: [
    {
      id: '0437754f-ab46-437d-9e65-1e56d0e59e36',
      type: DimensionType.Date,
      extractor: { type: 'financial', yearFormat: 'YYYYYY' },
      joinColumn: 'date_code',
      factTableColumn: 'YearCode',
      isSliceDimension: false
    },
    {
      id: 'fe7f8759-1ab3-484b-8d35-273d82786e87',
      type: DimensionType.ReferenceData,
      extractor: { categories: ['Geog/ITL1', 'Geog/LA'] },
      joinColumn: 'reference_data.item_id',
      factTableColumn: 'AreaCode',
      isSliceDimension: false
    },
    {
      id: '0a7b9a44-711c-4172-ae8a-ef598460c37c',
      type: DimensionType.LookupTable,
      extractor: {
        sortColumn: 'sort_order',
        isSW2Format: true,
        notesColumns: [
          { lang: 'en-gb', name: 'Notes_en' },
          { lang: 'cy-gb', name: 'Notes_cy' }
        ],
        tableLanguage: 'en-GB',
        descriptionColumns: [
          { lang: 'en-gb', name: 'Description_en' },
          { lang: 'cy-gb', name: 'Description_cy' }
        ]
      },
      joinColumn: 'RowRefAlt',
      factTableColumn: 'RowRef',
      isSliceDimension: false
    }
  ],
  publishedRevision: {
    id: '709e463a-c6b3-45fa-91a3-88d432764f6b',
    revisionIndex: 0,
    metadata: [
      {
        language: 'cy-GB',
        title: 'Testing-cy',
        summary: 'Lorem ipsum-cy',
        collection: 'Lorem ipsum-cy',
        quality: 'Lorem ipsum -cy',
        createdAt: '2025-05-01 13:10:40.208537+00',
        updatedAt: '2025-05-01 13:18:51.287+00'
      },
      {
        language: 'en-GB',
        title: 'Lighthouse Testing [DO NOT EDIT]',
        summary: 'Lorem ipsum',
        collection: 'Lorem ipsum',
        quality: 'Lorem ipsum',
        createdAt: '2025-05-01 13:10:40.208537+00',
        updatedAt: '2025-05-01 13:18:51.287+00'
      }
    ],
    createdAt: '2025-05-01 13:10:40.20187+00',
    updatedAt: '2025-05-01 13:19:12.491622+00',
    approvedAt: '2025-05-01 13:19:12.489+00',
    publishAt: '2025-05-01 13:20:00+00',
    onlineCubeFilename: '709e463a-c6b3-45fa-91a3-88d432764f6b-protocube.duckdb',
    roundingApplied: false,
    updateFrequency: 'NEVER',
    designation: Designation.Official,
    relatedLinks: [
      {
        id: 'AYPe',
        url: 'https://example.com',
        labelCY: 'Example link-cy',
        labelEN: 'Example link',
        created_at: '2025-05-01T13:15:44.030Z'
      }
    ],
    revisionTopics: [
      {
        topicId: 71
      },
      {
        topicId: 80
      }
    ],
    revisionProviders: [
      {
        groupId: '24bf9f9c-898a-4d23-ae1e-35a6ff30ee63',
        language: 'en-gb',
        createdAt: '2025-05-01 13:15:22.384+00'
      },
      {
        groupId: '24bf9f9c-898a-4d23-ae1e-35a6ff30ee63',
        language: 'cy-gb',
        createdAt: '2025-05-01 13:15:22.384+00'
      }
    ],
    createdBy: user,
    approvedBy: user
  },
  userGroupId: '24bf9f9c-898a-4d23-ae1e-35a6ff30ee63'
};

export default class DatasetSeeder extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    await this.seedUser(dataSource);
    await this.seedDataset(dataSource);
  }

  async seedUser(dataSource: DataSource): Promise<void> {
    console.log(`Seeding user...`);
    const entityManager = dataSource.createEntityManager();

    const users = entityManager.create(User, user);
    await dataSource.getRepository(User).save(users);
  }

  async seedDataset(dataSource: DataSource): Promise<void> {
    console.log(`Seeding dataset...`);
    const entityManager = dataSource.createEntityManager();

    try {
      let revision = approvedDataset.publishedRevision;
      const partialDataset = omit(approvedDataset, 'publishedRevision', 'dimensions', 'measure');
      const dataset = await entityManager.getRepository(Dataset).create(partialDataset).save();

      const dataFile = {
        originalname: 'QryHLTH1250_Data.csv',
        mimetype: 'text/csv',
        path: path.join(__dirname, 'resources', 'QryHLTH1250_Data.csv')
      } as TempFile;
      const dataTable = await validateAndUpload(dataFile, dataset.id, 'data_table');

      const rowRefFile = {
        originalname: 'QryHLTH1250_RowRef-fixed.csv',
        mimetype: 'text/csv',
        path: path.join(__dirname, 'resources', 'QryHLTH1250_RowRef-fixed.csv')
      } as TempFile;
      const protoRowRefLookupTable = await validateAndUpload(rowRefFile, dataset.id, 'lookup_table');

      const measureFile = {
        originalname: 'QryHLTH1250_Measure-fixed.csv',
        mimetype: 'text/csv',
        path: path.join(__dirname, 'resources', 'QryHLTH1250_Measure-fixed.csv')
      } as TempFile;
      const protoMeasureLookupTable = await validateAndUpload(measureFile, dataset.id, 'lookup_table');

      const provider = await entityManager.getRepository(Provider).findOne({
        where: { name: 'British Transport Police' }
      });

      if (!provider) {
        throw new Error('Provider not found');
      }

      revision = await entityManager.getRepository(Revision).save({
        ...revision,
        dataset,
        dataTable,
        revisionProviders: revision?.revisionProviders?.map((r) => {
          return { ...r, providerId: provider!.id };
        }),
        approvedBy: dataset.createdBy,
        approvedAt: dataset.live || undefined,
        publishAt: dataset.live || undefined
      });

      const duckdbFiles = [
        '709e463a-c6b3-45fa-91a3-88d432764f6b_cy.parquet',
        '709e463a-c6b3-45fa-91a3-88d432764f6b_en.parquet',
        '709e463a-c6b3-45fa-91a3-88d432764f6b-protocube.duckdb'
      ];

      const fileService = getFileService();

      for (const file of duckdbFiles) {
        const uploadBuffer = await readFile(path.join(__dirname, `./resources/${file}`));
        await fileService.saveBuffer(file, dataset.id, uploadBuffer);
      }

      await entityManager.getRepository(Dataset).save({
        ...dataset,
        dimensions: approvedDataset.dimensions!.map((d) => {
          if (d.type === DimensionType.LookupTable) {
            return {
              ...d,
              lookupTable: { ...convertDataTableToLookupTable(protoRowRefLookupTable), isStatsWales2Format: true }
            };
          }
          return d;
        }),
        measure: {
          ...approvedDataset.measure,
          lookupTable: { ...convertDataTableToLookupTable(protoMeasureLookupTable), isStatsWales2Format: true }
        },
        startRevision: revision,
        endRevision: revision,
        draftRevision: dataset.live ? undefined : revision,
        publishedRevision: dataset.live ? revision : undefined
      });
    } catch (err) {
      console.error(err, `Error seeding dataset ${approvedDataset.id}`);
      process.exit(1);
    }
  }
}
