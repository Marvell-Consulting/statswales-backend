import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { MAX_PAGE_SIZE } from '../../../../src/utils/page-defaults';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset, seedTopic } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('listings test user');
let userGroup = getTestUserGroup('Listings Test Group');

// IDs are fixed so tests can target specific datasets by id if needed
const DS_HEALTH_1 = '11111111-1111-4111-8111-111111111111';
const REV_HEALTH_1 = '11111111-1111-4111-8111-aaaaaaaaaaaa';
const DT_HEALTH_1 = '11111111-1111-4111-8111-bbbbbbbbbbbb';

const DS_HEALTH_2 = '22222222-2222-4222-8222-222222222222';
const REV_HEALTH_2 = '22222222-2222-4222-8222-aaaaaaaaaaaa';
const DT_HEALTH_2 = '22222222-2222-4222-8222-bbbbbbbbbbbb';

const DS_TRANSPORT = '33333333-3333-4333-8333-333333333333';
const REV_TRANSPORT = '33333333-3333-4333-8333-aaaaaaaaaaaa';
const DT_TRANSPORT = '33333333-3333-4333-8333-bbbbbbbbbbbb';

// A fourth, unpublished dataset is attached to a topic whose only dataset is unpublished.
// That topic must NOT appear in /topic.
const DS_UNPUBLISHED = '44444444-4444-4444-8444-444444444444';
const REV_UNPUBLISHED = '44444444-4444-4444-8444-aaaaaaaaaaaa';
const DT_UNPUBLISHED = '44444444-4444-4444-8444-bbbbbbbbbbbb';

// Topic hierarchy we seed (mirrors the real CSV's path convention):
//   Health (id 100, path "100")
//     └─ Dental (id 101, path "100.101")   ← leaf, 2 datasets
//   Transport (id 200, path "200")            ← has 1 dataset directly, no children
//   Fisheries (id 300, path "300")            ← only an unpublished dataset → must not appear
const TOPIC_HEALTH = 100;
const TOPIC_DENTAL = 101;
const TOPIC_TRANSPORT = 200;
const TOPIC_FISHERIES = 300;

const DAYS = (n: number) => new Date(Date.now() - n * 24 * 60 * 60 * 1000);

describe('Consumer V1 — listings (/, /topic, /topic/:topic_id)', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());

    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    await seedTopic({ id: TOPIC_HEALTH, path: `${TOPIC_HEALTH}`, nameEN: 'Health', nameCY: 'Iechyd' });
    await seedTopic({
      id: TOPIC_DENTAL,
      path: `${TOPIC_HEALTH}.${TOPIC_DENTAL}`,
      nameEN: 'Dental services',
      nameCY: 'Gwasanaethau deintyddol'
    });
    await seedTopic({ id: TOPIC_TRANSPORT, path: `${TOPIC_TRANSPORT}`, nameEN: 'Transport', nameCY: 'Trafnidiaeth' });
    await seedTopic({ id: TOPIC_FISHERIES, path: `${TOPIC_FISHERIES}`, nameEN: 'Fisheries', nameCY: 'Pysgodfeydd' });

    // Published datasets. Distinct firstPublishedAt so ordering is deterministic (desc by date).
    await seedPublishedDataset({
      user,
      datasetId: DS_HEALTH_1,
      revisionId: REV_HEALTH_1,
      dataTableId: DT_HEALTH_1,
      title: { en: 'Dental appointments 2023', cy: 'Apwyntiadau deintyddol 2023' },
      topicIds: [TOPIC_DENTAL],
      publishAt: DAYS(3),
      firstPublishedAt: DAYS(30)
    });

    await seedPublishedDataset({
      user,
      datasetId: DS_HEALTH_2,
      revisionId: REV_HEALTH_2,
      dataTableId: DT_HEALTH_2,
      title: { en: 'Dental appointments 2024', cy: 'Apwyntiadau deintyddol 2024' },
      topicIds: [TOPIC_DENTAL],
      publishAt: DAYS(2),
      firstPublishedAt: DAYS(10)
    });

    await seedPublishedDataset({
      user,
      datasetId: DS_TRANSPORT,
      revisionId: REV_TRANSPORT,
      dataTableId: DT_TRANSPORT,
      title: { en: 'Bus journeys', cy: 'Teithiau bws' },
      topicIds: [TOPIC_TRANSPORT],
      publishAt: DAYS(1),
      firstPublishedAt: DAYS(1)
    });

    // An unpublished dataset (no Revision row at all). We still want a Dataset row attached
    // to Fisheries so the topic would appear IF the API wrongly leaked unpublished data.
    // Easiest: seed a dataset with firstPublishedAt in the FUTURE — it is not yet visible.
    await seedPublishedDataset({
      user,
      datasetId: DS_UNPUBLISHED,
      revisionId: REV_UNPUBLISHED,
      dataTableId: DT_UNPUBLISHED,
      title: { en: 'Future fisheries dataset', cy: 'Set ddata pysgodfeydd yn y dyfodol' },
      topicIds: [TOPIC_FISHERIES],
      publishAt: new Date(Date.now() + 24 * 60 * 60 * 1000), // tomorrow
      firstPublishedAt: new Date(Date.now() + 24 * 60 * 60 * 1000)
    });
  }, 120_000);

  describe('GET /v1 — list all published datasets', () => {
    it('returns all published datasets, excludes future-publish ones', async () => {
      const res = await request(app).get('/v1');
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);

      // Matches OpenAPI DatasetsWithCount: { data: DatasetListItem[], count: integer }
      expect(res.body).toHaveProperty('data');
      expect(res.body).toHaveProperty('count');
      expect(Array.isArray(res.body.data)).toBe(true);

      const ids = res.body.data.map((d: { id: string }) => d.id);
      expect(ids).toContain(DS_HEALTH_1);
      expect(ids).toContain(DS_HEALTH_2);
      expect(ids).toContain(DS_TRANSPORT);
      expect(ids).not.toContain(DS_UNPUBLISHED);

      expect(res.body.count).toBe(3);
    });

    it('each item has the keys documented in DatasetListItem', async () => {
      const res = await request(app).get('/v1');
      const item = res.body.data.find((d: { id: string }) => d.id === DS_HEALTH_1);
      expect(item).toBeDefined();
      expect(typeof item.id).toBe('string');
      expect(typeof item.title).toBe('string');
      expect(typeof item.first_published_at).toBe('string');
      expect(typeof item.last_updated_at).toBe('string');
      // archived_at is optional; if present it should be a string, else absent
      if ('archived_at' in item) {
        expect(item.archived_at === null || typeof item.archived_at === 'string').toBe(true);
      }
    });

    it('returns English titles by default', async () => {
      const res = await request(app).get('/v1');
      const h1 = res.body.data.find((d: { id: string }) => d.id === DS_HEALTH_1);
      expect(h1.title).toBe('Dental appointments 2023');
    });

    it('returns Welsh titles when lang=cy', async () => {
      const res = await request(app).get('/v1').query({ lang: 'cy' });
      const h1 = res.body.data.find((d: { id: string }) => d.id === DS_HEALTH_1);
      expect(h1.title).toBe('Apwyntiadau deintyddol 2023');
    });

    it('orders datasets by first_published_at DESC', async () => {
      const res = await request(app).get('/v1');
      const ids = res.body.data.map((d: { id: string }) => d.id);
      // Transport (1d ago) < Health2 (10d ago) < Health1 (30d ago) → DESC: Transport, Health2, Health1
      expect(ids).toEqual([DS_TRANSPORT, DS_HEALTH_2, DS_HEALTH_1]);
    });

    it('honours page_size', async () => {
      const res = await request(app).get('/v1').query({ page_size: 1 });
      expect(res.status).toBe(200);
      expect(res.body.data.length).toBe(1);
      expect(res.body.count).toBe(3); // count reflects total, not page
    });

    it('honours page_number', async () => {
      const p1 = await request(app).get('/v1').query({ page_size: 1, page_number: 1 });
      const p2 = await request(app).get('/v1').query({ page_size: 1, page_number: 2 });
      expect(p1.body.data[0].id).not.toBe(p2.body.data[0].id);
    });

    it('clamps page_size above MAX_PAGE_SIZE silently', async () => {
      const res = await request(app)
        .get('/v1')
        .query({ page_size: MAX_PAGE_SIZE + 10 });
      expect(res.status).toBe(200);
      // should not throw, should return up to MAX_PAGE_SIZE items (we only have 3)
      expect(res.body.data.length).toBe(3);
    });
  });

  describe('GET /v1/topic — root topics', () => {
    it('returns the root-topics payload shape documented in OpenAPI (RootTopics)', async () => {
      const res = await request(app).get('/v1/topic');
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.body).toHaveProperty('children');
      expect(Array.isArray(res.body.children)).toBe(true);
    });

    it('includes only root topics that have a published dataset directly or via descendants', async () => {
      const res = await request(app).get('/v1/topic');
      const rootIds = res.body.children.map((t: { id: number }) => t.id);
      // Health (via Dental descendant) + Transport (direct) → yes
      // Fisheries (only unpublished dataset) → no
      expect(rootIds).toContain(TOPIC_HEALTH);
      expect(rootIds).toContain(TOPIC_TRANSPORT);
      expect(rootIds).not.toContain(TOPIC_FISHERIES);
      // Dental (id 101) is NOT a root — should not appear here
      expect(rootIds).not.toContain(TOPIC_DENTAL);
    });

    it('each topic has id, path, name, name_en, name_cy', async () => {
      const res = await request(app).get('/v1/topic');
      const health = res.body.children.find((t: { id: number }) => t.id === TOPIC_HEALTH);
      expect(health).toBeDefined();
      expect(health.id).toBe(TOPIC_HEALTH);
      expect(health.path).toBe(`${TOPIC_HEALTH}`);
      expect(health.name_en).toBe('Health');
      expect(health.name_cy).toBe('Iechyd');
      // `name` = the current-language name. Default lang is en-gb.
      expect(health.name).toBe('Health');
    });

    it('selects Welsh name for the `name` field when lang=cy', async () => {
      const res = await request(app).get('/v1/topic').query({ lang: 'cy' });
      const health = res.body.children.find((t: { id: number }) => t.id === TOPIC_HEALTH);
      expect(health.name).toBe('Iechyd');
    });

    it('selectedTopic/parents/datasets absent for root-topic listing', async () => {
      const res = await request(app).get('/v1/topic');
      expect(res.body.selectedTopic).toBeUndefined();
      expect(res.body.parents).toBeUndefined();
      expect(res.body.datasets).toBeUndefined();
    });
  });

  describe('GET /v1/topic/:topic_id — sub-topics / leaf datasets', () => {
    it('for a non-leaf topic returns children + parents, no datasets', async () => {
      const res = await request(app).get(`/v1/topic/${TOPIC_HEALTH}`);
      expect(res.status).toBe(200);
      expect(res.body.selectedTopic?.id).toBe(TOPIC_HEALTH);
      expect(Array.isArray(res.body.children)).toBe(true);
      const childIds = res.body.children.map((c: { id: number }) => c.id);
      expect(childIds).toContain(TOPIC_DENTAL);
      // Non-leaf → no datasets
      expect(res.body.datasets).toBeUndefined();
      // Parents: Health is a root topic, so parents should be an empty array or undefined
      expect(res.body.parents === undefined || Array.isArray(res.body.parents)).toBe(true);
    });

    it('for a leaf topic returns paginated datasets tagged directly to it', async () => {
      const res = await request(app).get(`/v1/topic/${TOPIC_DENTAL}`);
      expect(res.status).toBe(200);
      expect(res.body.selectedTopic?.id).toBe(TOPIC_DENTAL);
      // Dental has no children published → children should be empty
      expect(Array.isArray(res.body.children)).toBe(true);
      expect(res.body.children.length).toBe(0);
      expect(res.body.datasets).toBeDefined();
      const datasetIds = res.body.datasets.data.map((d: { id: string }) => d.id);
      expect(datasetIds.sort()).toEqual([DS_HEALTH_1, DS_HEALTH_2].sort());
      expect(res.body.datasets.count).toBe(2);
    });

    it('includes parents array for a sub-topic', async () => {
      const res = await request(app).get(`/v1/topic/${TOPIC_DENTAL}`);
      expect(Array.isArray(res.body.parents)).toBe(true);
      const parentIds = res.body.parents.map((p: { id: number }) => p.id);
      expect(parentIds).toContain(TOPIC_HEALTH);
    });

    it('sort_by=title ASC is respected', async () => {
      const res = await request(app)
        .get(`/v1/topic/${TOPIC_DENTAL}`)
        .query({ sort_by: JSON.stringify([{ columnName: 'title', direction: 'ASC' }]) });
      expect(res.status).toBe(200);
      const titles = res.body.datasets.data.map((d: { title: string }) => d.title);
      expect(titles).toEqual([...titles].sort((a: string, b: string) => a.localeCompare(b)));
    });

    it('sort_by on a disallowed column → 400', async () => {
      const res = await request(app)
        .get(`/v1/topic/${TOPIC_DENTAL}`)
        .query({ sort_by: JSON.stringify([{ columnName: 'id', direction: 'ASC' }]) });
      expect(res.status).toBe(400);
    });

    it('non-existent numeric topic_id → 404', async () => {
      const res = await request(app).get('/v1/topic/999999');
      expect(res.status).toBe(404);
    });

    it('non-numeric topic_id → 404', async () => {
      const res = await request(app).get('/v1/topic/abc');
      expect(res.status).toBe(404);
    });

    // The topic_id validator uses /\d+/ without anchors, so "1abc" matches the regex and the
    // controller then parseInt's it into 1. Intended behaviour would be "treat non-integer as
    // invalid and return 404". We assert the intended behaviour.
    it('topic_id with trailing garbage (e.g. "100abc") should be rejected', async () => {
      const res = await request(app).get(`/v1/topic/${TOPIC_HEALTH}abc`);
      expect(res.status).toBe(404);
    });

    // consumer.ts:246 hardcodes pageSize = 1000 regardless of query param. Documented params
    // include page_size — the user should be able to shrink the page. Intended behaviour
    // assertion.
    it('page_size query param is respected for leaf-topic datasets', async () => {
      const res = await request(app).get(`/v1/topic/${TOPIC_DENTAL}`).query({ page_size: 1 });
      expect(res.status).toBe(200);
      expect(res.body.datasets.data.length).toBe(1);
      expect(res.body.datasets.count).toBe(2);
    });

    it('root listing endpoint omits the topic whose only dataset is unpublished', async () => {
      const res = await request(app).get(`/v1/topic/${TOPIC_FISHERIES}`);
      // Either 404 (topic has nothing published, treated as missing) OR 200 with empty
      // children+no datasets. The OpenAPI doesn't specify. Assert either way + flag.
      if (res.status === 200) {
        const hasData = res.body.datasets && res.body.datasets.data && res.body.datasets.data.length > 0;
        expect(hasData).toBe(false);
      } else {
        expect(res.status).toBe(404);
      }
    });
  });
});
