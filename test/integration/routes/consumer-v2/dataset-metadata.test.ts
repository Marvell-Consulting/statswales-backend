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

const user: User = getTestUser('v2 metadata test user');
let userGroup = getTestUserGroup('V2 Metadata Test Group');

const DATASET_ID = 'aaaaaaaa-aaaa-4aaa-8aa2-aaaaaaaaaaaa';
const REV_1_ID = 'aaaaaaaa-aaaa-4aaa-8aa2-aaaaaaaa0001';
const REV_2_ID = 'aaaaaaaa-aaaa-4aaa-8aa2-aaaaaaaa0002';
const DT_1_ID = 'aaaaaaaa-aaaa-4aaa-8aa2-dddddddd0001';
const DT_2_ID = 'aaaaaaaa-aaaa-4aaa-8aa2-dddddddd0002';

// Second dataset without a user group — exercises the absent-publisher branch.
const LOOSE_DATASET_ID = 'bbbbbbbb-bbbb-4bbb-8bb2-bbbbbbbbbbbb';
const LOOSE_REV_ID = 'bbbbbbbb-bbbb-4bbb-8bb2-aaaaaaaaaaaa';
const LOOSE_DT_ID = 'bbbbbbbb-bbbb-4bbb-8bb2-bbbbbbbbbbbb';

// A third dataset, used to prove that a revision that doesn't belong to the URL's dataset
// is rejected by ensurePublishedRevision.
const OTHER_DATASET_ID = 'cccccccc-cccc-4ccc-8cc2-cccccccccccc';
const OTHER_REV_ID = 'cccccccc-cccc-4ccc-8cc2-aaaaaaaaaaaa';
const OTHER_DT_ID = 'cccccccc-cccc-4ccc-8cc2-bbbbbbbbbbbb';

const DAYS = (n: number) => new Date(Date.now() - n * 24 * 60 * 60 * 1000);

describe('Consumer V2 — dataset metadata (/:dataset_id, /:dataset_id/revision/:revision_id)', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());

    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

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

    await seedPublishedDataset({
      user,
      datasetId: LOOSE_DATASET_ID,
      revisionId: LOOSE_REV_ID,
      dataTableId: LOOSE_DT_ID,
      skipUserGroup: true,
      publishAt: DAYS(1),
      firstPublishedAt: DAYS(1)
    });

    await seedPublishedDataset({
      user,
      datasetId: OTHER_DATASET_ID,
      revisionId: OTHER_REV_ID,
      dataTableId: OTHER_DT_ID,
      publishAt: DAYS(5),
      firstPublishedAt: DAYS(5)
    });
  }, 120_000);

  describe('GET /v2/:dataset_id — dataset metadata', () => {
    it('returns 200 with the top-level keys documented in ConsumerDatasetDTO', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.body.id).toBe(DATASET_ID);
      expect(res.body).toHaveProperty('first_published_at');
      expect(res.body).toHaveProperty('published_revision');
      // v2's Dataset DTO intentionally omits the full revisions[] array that v1 includes —
      // the OpenAPI schema (src/routes/consumer/v2/openapi-en.json) only defines
      // published_revision. History is available via the (hidden) /revision/:revision_id.
      expect(res.body).not.toHaveProperty('revisions');
    });

    it('published_revision points at the latest (REV_2_ID)', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}`);
      expect(res.body.published_revision.id).toBe(REV_2_ID);
      expect(res.body.published_revision.revision_index).toBe(2);
    });

    it('published_revision.metadata contains both en-GB and cy-GB entries', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}`);
      const meta = res.body.published_revision.metadata as Array<{ language: string; title: string }>;
      const en = meta.find((m) => m.language.toLowerCase().startsWith('en'));
      const cy = meta.find((m) => m.language.toLowerCase().startsWith('cy'));
      expect(en?.title).toBe('Population estimates (updated)');
      expect(cy?.title).toBe('Amcangyfrifon poblogaeth (wedi diweddaru)');
    });

    it('dataset with linked user group returns a `publisher` block', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}`);
      expect(res.body.publisher).toBeDefined();
    });

    it('dataset without a linked user group omits the `publisher` block', async () => {
      const res = await request(app).get(`/v2/${LOOSE_DATASET_ID}`);
      expect(res.status).toBe(200);
      expect(res.body.publisher).toBeUndefined();
    });

    it('optional fields on the DTO are omitted (undefined) rather than null', async () => {
      const res = await request(app).get(`/v2/${LOOSE_DATASET_ID}`);
      expect(res.body).not.toHaveProperty('archived_at');
      expect(res.body).not.toHaveProperty('replaced_by');
      expect(res.body).not.toHaveProperty('start_date');
      expect(res.body).not.toHaveProperty('end_date');
      expect(res.body.published_revision).not.toHaveProperty('unpublished_at');
    });

    it('returns 404 for a malformed UUID', async () => {
      const res = await request(app).get('/v2/not-a-uuid');
      expect(res.status).toBe(404);
    });

    it('returns 404 for a well-formed UUID that does not exist', async () => {
      const res = await request(app).get('/v2/99999999-9999-4999-8999-999999999999');
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v2/:dataset_id/revision/:revision_id — single-revision metadata (undocumented)', () => {
    // NOTE: swagger-ignored at src/routes/consumer/v2/api.ts:206. Behaviour recorded here
    // so we can decide later whether to document, keep hidden, or remove.
    it('returns 200 with SingleLanguageRevisionDTO shape for a valid dataset/revision pair', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/revision/${REV_2_ID}`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.body.id).toBe(REV_2_ID);
      expect(res.body.revision_index).toBe(2);
      expect(typeof res.body.updated_at).toBe('string');
      expect(typeof res.body.publish_at).toBe('string');
      // metadata is language-filtered (single entry, not an array like /:dataset_id)
      expect(res.body.metadata).toBeDefined();
      expect(Array.isArray(res.body.metadata)).toBe(false);
    });

    it('metadata is English by default', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/revision/${REV_2_ID}`);
      expect(res.body.metadata.title).toBe('Population estimates (updated)');
    });

    it('metadata is Welsh when lang=cy', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/revision/${REV_2_ID}`).query({ lang: 'cy' });
      expect(res.body.metadata.title).toBe('Amcangyfrifon poblogaeth (wedi diweddaru)');
    });

    it('returns 404 for a revision that does not belong to the URL dataset', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/revision/${OTHER_REV_ID}`);
      expect(res.status).toBe(404);
    });

    it('returns 404 for a malformed revision UUID', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/revision/not-a-uuid`);
      expect(res.status).toBe(404);
    });

    it('returns 404 for a well-formed revision UUID that does not exist', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/revision/99999999-9999-4999-8999-999999999999`);
      expect(res.status).toBe(404);
    });

    it('returns 404 for a malformed dataset UUID (ensurePublishedDataset guard)', async () => {
      const res = await request(app).get(`/v2/not-a-uuid/revision/${REV_2_ID}`);
      expect(res.status).toBe(404);
    });

    it('optional revision fields are omitted rather than null', async () => {
      const res = await request(app).get(`/v2/${LOOSE_DATASET_ID}/revision/${LOOSE_REV_ID}`);
      expect(res.status).toBe(200);
      // Per project convention unset optional fields should be absent, not null.
      expect(res.body).not.toHaveProperty('unpublished_at');
      expect(res.body).not.toHaveProperty('rounding_description');
      expect(res.body).not.toHaveProperty('designation');
    });
  });
});
