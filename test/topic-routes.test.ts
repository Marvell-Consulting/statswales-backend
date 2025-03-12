import request from 'supertest';

import app from '../src/app';
import { initDb } from '../src/db/init';
import DatabaseManager from '../src/db/database-manager';
import { initPassport } from '../src/middleware/passport-auth';
import { User } from '../src/entities/user/user';
import { Topic } from '../src/entities/dataset/topic';
import { TopicDTO } from '../src/dtos/topic-dto';
import { Locale } from '../src/enums/locale';

import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

const user: User = getTestUser('test', 'user');

const topics: Partial<Topic>[] = [
  { id: 1, path: '1', nameEN: 'Topic 1', nameCY: 'Pwnc 1' },
  { id: 2, path: '1.1', nameEN: 'Topic 1.1', nameCY: 'Pwnc 1.1' },
  { id: 3, path: '1.2', nameEN: 'Topic 1.2', nameCY: 'Pwnc 1.2' },
  { id: 4, path: '1.3', nameEN: 'Topic 1.3', nameCY: 'Pwnc 1.3' },
  { id: 5, path: '5', nameEN: 'Topic 5', nameCY: 'Pwnc 5' },
  { id: 6, path: '5.1', nameEN: 'Topic 5.1', nameCY: 'Pwnc 5.1' },
  { id: 7, path: '7', nameEN: 'Topic 7', nameCY: 'Pwnc 7' }
];

describe('Topics', () => {
  let dbManager: DatabaseManager;

  beforeAll(async () => {
    try {
      dbManager = await initDb();
      await initPassport(dbManager.getDataSource());
      await user.save();
      await dbManager.getEntityManager().save(Topic, topics);
    } catch (_err) {
      await dbManager.getDataSource().dropDatabase();
      await dbManager.getDataSource().destroy();
      process.exit(1);
    }
  });

  test('Get all Topics', async () => {
    const res = await request(app).get('/topic').set(getAuthHeader(user));
    const expected = topics.map((topic) => TopicDTO.fromTopic(topic as Topic, Locale.English));

    expect(res.status).toBe(200);
    expect(res.body).toEqual(expected);
  });

  afterAll(async () => {
    await dbManager.getDataSource().dropDatabase();
    await dbManager.getDataSource().destroy();
  });
});
