import express, { Request, Response } from 'express';
import request from 'supertest';

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

jest.mock('../../../src/config', () => ({
  config: {
    session: {
      store: 'memory',
      secret: 'test-secret',
      secure: false,
      maxAge: 86400000
    }
  }
}));

import sessionMiddleware, { getSessionStoreStatus, SessionStoreStatus } from '../../../src/middleware/session';

describe('session middleware (memory store)', () => {
  test('passes requests through when store is healthy', async () => {
    const app = express();
    app.use(sessionMiddleware);
    app.get('/test', (_req: Request, res: Response) => {
      res.status(200).json({ message: 'ok' });
    });

    const res = await request(app).get('/test');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ message: 'ok' });
  });

  test('getSessionStoreStatus returns memory type with connected true', () => {
    const status: SessionStoreStatus = getSessionStoreStatus();
    expect(status).toEqual({ type: 'memory', connected: true });
  });
});
