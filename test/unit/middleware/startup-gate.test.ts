import express from 'express';
import request from 'supertest';

import { startupGate } from '../../../src/middleware/startup-gate';
import { markStartupComplete, markStartupPending } from '../../../src/utils/startup';

const createApp = () => {
  const app = express();
  app.use(startupGate);
  app.get('/dataset', (_req, res) => {
    res.json({ ok: true });
  });
  return app;
};

describe('startupGate', () => {
  test('returns 503 while startup is pending, without calling the route', async () => {
    markStartupPending();
    const res = await request(createApp()).get('/dataset');
    expect(res.status).toBe(503);
    expect(res.body).toEqual({ error: 'starting' });
  });

  test('passes requests on once startup is complete', async () => {
    markStartupComplete();
    const res = await request(createApp()).get('/dataset');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });
});
