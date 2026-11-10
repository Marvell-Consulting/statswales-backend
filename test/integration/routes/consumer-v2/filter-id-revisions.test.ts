import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { Revision } from '../../../../src/entities/dataset/revision';
import { GroupRole } from '../../../../src/enums/group-role';
import { FactTableColumnType } from '../../../../src/enums/fact-table-column-type';
import { FRONTEND_DATA_OPTIONS } from '../../../../src/dtos/data-options-dto';
import { QueryStoreRepository } from '../../../../src/repositories/query-store';
import BlobStorage from '../../../../src/services/blob-storage';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset } from '../../../helpers/seed-published-dataset';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('v2 filter id revisions test user');
let userGroup = getTestUserGroup('V2 Filter ID Revisions Test Group');

const AREAS = ['W06000001', 'W06000002'];
const YEARS = [2022, 2023];
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

const PUBLISHED = { dataset: 'ddddddd1-dddd-4ddd-8dd1-dddddddddddd', revision: 'ddddddd1-dddd-4ddd-8dd1-aaaaaaaaaaaa' };
const SCHEDULED = { dataset: 'ddddddd2-dddd-4ddd-8dd2-dddddddddddd', revision: 'ddddddd2-dddd-4ddd-8dd2-aaaaaaaaaaaa' };
const WITHDRAWN = { dataset: 'ddddddd3-dddd-4ddd-8dd3-dddddddddddd', revision: 'ddddddd3-dddd-4ddd-8dd3-aaaaaaaaaaaa' };

async function seed(ids: { dataset: string; revision: string }, prefix: string, value: number, publishAt?: Date) {
  await seedPublishedDataset({
    user,
    datasetId: ids.dataset,
    revisionId: ids.revision,
    dataTableId: `${prefix}-dddd-4ddd-8ddd-bbbbbbbbbbbb`,
    publishAt,
    factColumns: [
      { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
      { name: 'YearCode', datatype: 'BIGINT', columnType: FactTableColumnType.Dimension },
      { name: 'MeasureCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Measure },
      { name: 'Data', datatype: 'DOUBLE PRECISION', columnType: FactTableColumnType.DataValues },
      { name: 'NoteCode', datatype: 'VARCHAR', columnType: FactTableColumnType.NoteCodes }
    ],
    rowBuilder: (i: number) => [AREAS[Math.floor(i / YEARS.length)], YEARS[i % YEARS.length], 'count', value, null],
    rowCount: AREAS.length * YEARS.length,
    dimensions: [
      {
        factTableColumn: 'AreaCode',
        lookupTableId: `${prefix}-dddd-4ddd-8ddd-cccccccccccc`,
        name: { en: 'Area', cy: 'Ardal' },
        lookupRows: AREAS.flatMap((ref, idx) => [
          { reference: ref, language: 'en' as const, description: ref, sortOrder: idx },
          { reference: ref, language: 'cy' as const, description: ref, sortOrder: idx }
        ])
      },
      {
        factTableColumn: 'YearCode',
        lookupTableId: `${prefix}-dddd-4ddd-8ddd-eeeeeeeeeeee`,
        referenceDatatype: 'BIGINT',
        name: { en: 'Year', cy: 'Blwyddyn' },
        lookupRows: YEARS.flatMap((year, idx) => [
          { reference: year, language: 'en' as const, description: String(year), sortOrder: idx },
          { reference: year, language: 'cy' as const, description: String(year), sortOrder: idx }
        ])
      }
    ]
  });
}

async function filterFor(ids: { dataset: string; revision: string }, pivot = false): Promise<string> {
  const options = pivot ? { ...FRONTEND_DATA_OPTIONS, pivot: { x: 'AreaCode', y: 'YearCode' } } : FRONTEND_DATA_OPTIONS;
  const entry = await QueryStoreRepository.generate(ids.dataset, ids.revision, options);
  return entry.id;
}

describe('Consumer V2 — filter IDs are only served for published revisions', () => {
  let publishedFilterId: string;
  let scheduledFilterId: string;
  let scheduledPivotFilterId: string;
  let withdrawnFilterId: string;

  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport();

    userGroup = await dbManager.getPublisherDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    await seed(PUBLISHED, 'ddddddd1', 11.5);
    await seed(SCHEDULED, 'ddddddd2', 22.5, new Date(Date.now() + ONE_WEEK_MS));
    await seed(WITHDRAWN, 'ddddddd3', 33.5);

    publishedFilterId = await filterFor(PUBLISHED);
    scheduledFilterId = await filterFor(SCHEDULED);
    scheduledPivotFilterId = await filterFor(SCHEDULED, true);
    withdrawnFilterId = await filterFor(WITHDRAWN);

    await dbManager
      .getPublisherDataSource()
      .getRepository(Revision)
      .update({ id: WITHDRAWN.revision }, { unpublishedAt: new Date() });
  }, 180_000);

  it('serves a filter for the published revision of the dataset', async () => {
    const res = await request(app).get(`/v2/${PUBLISHED.dataset}/data/${publishedFilterId}`).query({ format: 'json' });
    expect(res.status).toBe(200);
    expect(JSON.stringify(res.body)).toContain('11.5');
  });

  describe('a filter for a revision that is not published yet', () => {
    it('GET /data/:filter_id returns 404', async () => {
      const res = await request(app)
        .get(`/v2/${PUBLISHED.dataset}/data/${scheduledFilterId}`)
        .query({ format: 'json' });
      expect(res.status).toBe(404);
      expect(JSON.stringify(res.body)).not.toContain('22.5');
    });

    it('GET /pivot/:filter_id returns 404', async () => {
      const res = await request(app)
        .get(`/v2/${PUBLISHED.dataset}/pivot/${scheduledPivotFilterId}`)
        .query({ format: 'json' });
      expect(res.status).toBe(404);
      expect(JSON.stringify(res.body)).not.toContain('22.5');
    });

    it('GET /data/:filter_id/pivot returns 404', async () => {
      const res = await request(app)
        .get(`/v2/${PUBLISHED.dataset}/data/${scheduledFilterId}/pivot`)
        .query({ x: 'AreaCode', y: 'YearCode', format: 'json' });
      expect(res.status).toBe(404);
      expect(JSON.stringify(res.body)).not.toContain('22.5');
    });

    it('GET /query/:filter_id returns 404', async () => {
      const res = await request(app).get(`/v2/${PUBLISHED.dataset}/query/${scheduledFilterId}`);
      expect(res.status).toBe(404);
    });
  });

  it('returns 404 for a filter whose revision has been unpublished', async () => {
    const res = await request(app).get(`/v2/${PUBLISHED.dataset}/data/${withdrawnFilterId}`).query({ format: 'json' });
    expect(res.status).toBe(404);
    expect(JSON.stringify(res.body)).not.toContain('33.5');
  });
});
