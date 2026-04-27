import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { SearchLog } from '../../../../src/entities/search-log';
import { SearchMode } from '../../../../src/enums/search-mode';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('v2 search test user');
let userGroup = getTestUserGroup('V2 Search Test Group');

const DS_DENTAL = '11111111-1111-4111-8311-111111111111';
const REV_DENTAL = '11111111-1111-4111-8311-aaaaaaaaaaaa';
const DT_DENTAL = '11111111-1111-4111-8311-bbbbbbbbbbbb';

const DS_BUSES = '22222222-2222-4222-8322-222222222222';
const REV_BUSES = '22222222-2222-4222-8322-aaaaaaaaaaaa';
const DT_BUSES = '22222222-2222-4222-8322-bbbbbbbbbbbb';

const DS_WELSH_ONLY = '33333333-3333-4333-8333-333333331113';
const REV_WELSH_ONLY = '33333333-3333-4333-8333-aaaaaaaaaa13';
const DT_WELSH_ONLY = '33333333-3333-4333-8333-bbbbbbbbbb13';

describe('Consumer V2 — GET /v2/search', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());

    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    await seedPublishedDataset({
      user,
      datasetId: DS_DENTAL,
      revisionId: REV_DENTAL,
      dataTableId: DT_DENTAL,
      title: { en: 'Dental appointments in Cardiff', cy: 'Apwyntiadau deintyddol yng Nghaerdydd' },
      summary: { en: 'Monthly dental activity across Wales', cy: 'Gweithgarwch deintyddol misol ledled Cymru' }
    });

    await seedPublishedDataset({
      user,
      datasetId: DS_BUSES,
      revisionId: REV_BUSES,
      dataTableId: DT_BUSES,
      title: { en: 'Bus journeys by local authority', cy: 'Teithiau bws fesul awdurdod lleol' },
      summary: { en: 'Quarterly bus passenger numbers', cy: 'Nifer y teithwyr bws chwarterol' }
    });

    // A dataset whose English title has no distinguishing word but whose Welsh title does —
    // lets us verify the search hits the correct language columns.
    await seedPublishedDataset({
      user,
      datasetId: DS_WELSH_ONLY,
      revisionId: REV_WELSH_ONLY,
      dataTableId: DT_WELSH_ONLY,
      title: { en: 'Generic population data', cy: 'Ffwlbriwlsyn poblogaeth' },
      summary: { en: 'Population counts by year', cy: 'Cyfrifon poblogaeth yn ôl blwyddyn' }
    });
  }, 120_000);

  describe('validation', () => {
    it('missing keywords → 400', async () => {
      const res = await request(app).get('/v2/search');
      expect(res.status).toBe(400);
    });

    it('empty keywords → 400', async () => {
      const res = await request(app).get('/v2/search').query({ keywords: '' });
      expect(res.status).toBe(400);
    });

    it('invalid mode → 400', async () => {
      const res = await request(app).get('/v2/search').query({ keywords: 'dental', mode: 'not-a-mode' });
      expect(res.status).toBe(400);
    });
  });

  describe('happy path across SearchMode variants', () => {
    const modes = [SearchMode.Basic, SearchMode.BasicSplit, SearchMode.FTS, SearchMode.FTSSimple, SearchMode.Fuzzy];

    for (const mode of modes) {
      it(`mode=${mode} returns SearchResultsWithCount shape and matches "dental"`, async () => {
        const res = await request(app).get('/v2/search').query({ keywords: 'dental', mode });
        expect(res.status).toBe(200);
        expect(res.body).toHaveProperty('data');
        expect(res.body).toHaveProperty('count');
        expect(Array.isArray(res.body.data)).toBe(true);
        const ids = res.body.data.map((d: { id: string }) => d.id);
        expect(ids).toContain(DS_DENTAL);
      });
    }

    it('defaults to basic mode when mode omitted', async () => {
      const res = await request(app).get('/v2/search').query({ keywords: 'dental' });
      expect(res.status).toBe(200);
      const ids = res.body.data.map((d: { id: string }) => d.id);
      expect(ids).toContain(DS_DENTAL);
    });

    it('items carry the documented SearchResultDTO fields', async () => {
      const res = await request(app).get('/v2/search').query({ keywords: 'dental' });
      const item = res.body.data.find((d: { id: string }) => d.id === DS_DENTAL);
      expect(item).toBeDefined();
      expect(typeof item.id).toBe('string');
      expect(typeof item.title).toBe('string');
    });
  });

  describe('bilingual search', () => {
    it('a Welsh-only distinctive word is found with lang=cy', async () => {
      const res = await request(app).get('/v2/search').query({ keywords: 'Ffwlbriwlsyn', lang: 'cy' });
      expect(res.status).toBe(200);
      const ids = res.body.data.map((d: { id: string }) => d.id);
      expect(ids).toContain(DS_WELSH_ONLY);
    });

    it('the same Welsh word is NOT found with lang=en (English title has no match)', async () => {
      const res = await request(app).get('/v2/search').query({ keywords: 'Ffwlbriwlsyn', lang: 'en' });
      expect(res.status).toBe(200);
      const ids = res.body.data.map((d: { id: string }) => d.id);
      expect(ids).not.toContain(DS_WELSH_ONLY);
    });
  });

  describe('SearchLog telemetry', () => {
    it('every successful search writes a SearchLog row with mode, keywords and result count', async () => {
      const before = await SearchLog.count();
      const res = await request(app).get('/v2/search').query({ keywords: 'bus-unique-keyword-xyz' });
      expect(res.status).toBe(200);
      const after = await SearchLog.count();
      expect(after).toBe(before + 1);

      const [log] = await SearchLog.find({ where: { keywords: 'bus-unique-keyword-xyz' } });
      expect(log).toBeDefined();
      expect(log.mode).toBe(SearchMode.Basic);
      expect(log.resultCount).toBe(res.body.count);
    });

    it('mode is recorded on the log when provided', async () => {
      await request(app).get('/v2/search').query({ keywords: 'fts-log-keyword', mode: SearchMode.FTS });
      const [log] = await SearchLog.find({ where: { keywords: 'fts-log-keyword' } });
      expect(log?.mode).toBe(SearchMode.FTS);
    });
  });
});
