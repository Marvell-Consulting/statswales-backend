import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { addPublishedRevision, seedPublishedDataset } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('metadata test user');
let userGroup = getTestUserGroup('Metadata Test Group');

const DATASET_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const REV_1_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0001';
const REV_2_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0002';
const DT_1_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-dddddddd0001';
const DT_2_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-dddddddd0002';

// Another dataset with no userGroup (publisher absent case) to exercise consumer.ts:52-55
const LOOSE_DATASET_ID = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const LOOSE_REV_ID = 'bbbbbbbb-bbbb-4bbb-8bbb-aaaaaaaaaaaa';
const LOOSE_DT_ID = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';

const DAYS = (n: number) => new Date(Date.now() - n * 24 * 60 * 60 * 1000);

describe('Consumer V1 — dataset metadata (/:dataset_id, /:dataset_id/history)', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());

    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    // Dataset with two published revisions so /history has >1 entry.
    await seedPublishedDataset({
      user,
      datasetId: DATASET_ID,
      revisionId: REV_1_ID,
      dataTableId: DT_1_ID,
      title: { en: 'Population estimates', cy: 'Amcangyfrifon poblogaeth' },
      summary: { en: 'Annual estimates for Wales', cy: 'Amcangyfrifon blynyddol ar gyfer Cymru' },
      publishAt: DAYS(10),
      firstPublishedAt: DAYS(10)
    });

    await addPublishedRevision({
      user,
      datasetId: DATASET_ID,
      revisionId: REV_2_ID,
      dataTableId: DT_2_ID,
      previousRevisionId: REV_1_ID,
      revisionIndex: 2,
      title: { en: 'Population estimates (updated)', cy: 'Amcangyfrifon poblogaeth (wedi diweddaru)' },
      summary: { en: 'Annual estimates for Wales — 2024 update', cy: 'Diweddariad 2024' },
      publishAt: DAYS(2)
    });

    // Second dataset without a user group — exercises the "no publisher block" branch.
    await seedPublishedDataset({
      user,
      datasetId: LOOSE_DATASET_ID,
      revisionId: LOOSE_REV_ID,
      dataTableId: LOOSE_DT_ID,
      skipUserGroup: true,
      publishAt: DAYS(1),
      firstPublishedAt: DAYS(1)
    });
  }, 120_000);

  describe('GET /v1/:dataset_id — dataset metadata', () => {
    it('returns 200 with the top-level keys documented in the ConsumerDatasetDTO', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.body.id).toBe(DATASET_ID);
      expect(res.body).toHaveProperty('first_published_at');
      expect(res.body).toHaveProperty('revisions');
      expect(res.body).toHaveProperty('published_revision');
      expect(Array.isArray(res.body.revisions)).toBe(true);
    });

    it('includes only published revisions in `revisions`', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}`);
      const ids = res.body.revisions.map((r: { id: string }) => r.id);
      expect(ids).toContain(REV_1_ID);
      expect(ids).toContain(REV_2_ID);
    });

    it('published_revision points at the latest (REV_2_ID)', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}`);
      expect(res.body.published_revision.id).toBe(REV_2_ID);
      expect(res.body.published_revision.revision_index).toBe(2);
    });

    it('published_revision.metadata contains both en-GB and cy-GB entries with correct titles', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}`);
      const meta = res.body.published_revision.metadata as Array<{ language: string; title: string }>;
      const en = meta.find((m) => m.language.toLowerCase().startsWith('en'));
      const cy = meta.find((m) => m.language.toLowerCase().startsWith('cy'));
      expect(en?.title).toBe('Population estimates (updated)');
      expect(cy?.title).toBe('Amcangyfrifon poblogaeth (wedi diweddaru)');
    });

    it('dataset with linked user group returns a `publisher` block', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}`);
      expect(res.body.publisher).toBeDefined();
    });

    it('dataset without a linked user group omits the `publisher` block', async () => {
      const res = await request(app).get(`/v1/${LOOSE_DATASET_ID}`);
      expect(res.status).toBe(200);
      expect(res.body.publisher).toBeUndefined();
    });

    it('optional fields on the DTO are omitted (undefined) rather than null', async () => {
      // Per project convention, unset optional fields should be absent, not null.
      const res = await request(app).get(`/v1/${LOOSE_DATASET_ID}`);
      // We don't enumerate every optional field; spot-check the known optional ones.
      expect(res.body).not.toHaveProperty('archived_at');
      expect(res.body).not.toHaveProperty('replaced_by');
      expect(res.body).not.toHaveProperty('start_date');
      expect(res.body).not.toHaveProperty('end_date');
      expect(res.body.published_revision).not.toHaveProperty('unpublished_at');
    });

    it('returns 404 for a malformed UUID', async () => {
      const res = await request(app).get('/v1/not-a-uuid');
      expect(res.status).toBe(404);
    });

    it('returns 404 for a well-formed UUID that does not exist', async () => {
      const res = await request(app).get('/v1/99999999-9999-4999-8999-999999999999');
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v1/:dataset_id/history — publication history (undocumented endpoint)', () => {
    // NOTE: This endpoint is hidden from OpenAPI docs via `#swagger.ignore = true`
    // (src/routes/consumer/v1/api.ts:145). Tests here record the actual shape and flag the fact
    // it's undocumented. Either the team should document it or remove it — the decision belongs
    // with the maintainers.
    it('returns 200 with an array of published revisions', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/history`);
      expect(res.status).toBe(200);
      expect(Array.isArray(res.body)).toBe(true);
      expect(res.body.length).toBe(2);
    });

    it('orders entries by publish_at DESC (newest first)', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/history`);
      const indexes = res.body.map((r: { revision_index: number }) => r.revision_index);
      expect(indexes).toEqual([2, 1]);
    });

    it('each entry has revision_index, publish_at, metadata', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/history`);
      for (const rev of res.body) {
        expect(typeof rev.id).toBe('string');
        expect(typeof rev.revision_index).toBe('number');
        expect(typeof rev.publish_at).toBe('string');
        expect(Array.isArray(rev.metadata)).toBe(true);
      }
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).get('/v1/99999999-9999-4999-8999-999999999999/history');
      expect(res.status).toBe(404);
    });

    it('returns 404 for a malformed UUID', async () => {
      const res = await request(app).get('/v1/not-a-uuid/history');
      expect(res.status).toBe(404);
    });
  });
});
