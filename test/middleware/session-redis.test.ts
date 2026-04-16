import express, { Request, Response } from 'express';
import request from 'supertest';

jest.mock('../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

jest.mock('../../src/config', () => ({
  config: {
    session: {
      store: 'redis',
      secret: 'test-secret',
      secure: false,
      maxAge: 86400000,
      redisUrl: 'redis://localhost',
      redisPassword: ''
    }
  }
}));

// Control the fake Redis client from tests
const fakeRedisClient = {
  isReady: true,
  on: jest.fn(),
  connect: jest.fn().mockResolvedValue(undefined)
};

jest.mock('redis', () => ({
  createClient: jest.fn(() => fakeRedisClient)
}));

// Control what error (if any) the session middleware calls back with
let storeError: Error | null = null;

jest.mock('express-session', () => {
  // Return a factory that produces controllable middleware
  const factory = (): express.RequestHandler => {
    return (_req, _res, cb) => {
      if (storeError) {
        cb(storeError);
      } else {
        cb();
      }
    };
  };
  factory.MemoryStore = jest.fn();
  // eslint-disable-next-line @typescript-eslint/naming-convention
  return { __esModule: true, default: factory, MemoryStore: factory.MemoryStore };
});

jest.mock('connect-redis', () => ({
  RedisStore: jest.fn()
}));

import sessionMiddleware, { getSessionStoreStatus } from '../../src/middleware/session';

function createApp(): express.Express {
  const errorCapture = jest.fn();
  const app = express();
  app.use(sessionMiddleware);
  app.get('/test', (_req: Request, res: Response) => {
    res.status(200).json({ message: 'ok' });
  });
  app.use((err: any, _req: Request, res: Response, _next: express.NextFunction) => {
    errorCapture(err);
    res.status(err.status || 500).json({ error: err.message });
  });
  (app as any).errorCapture = errorCapture;
  return app;
}

describe('session middleware (redis store)', () => {
  afterEach(() => {
    storeError = null;
    fakeRedisClient.isReady = true;
  });

  test('passes requests through when Redis is healthy', async () => {
    const app = createApp();
    const res = await request(app).get('/test');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ message: 'ok' });
  });

  test('returns 503 when Redis is not ready and store errors', async () => {
    fakeRedisClient.isReady = false;
    storeError = new Error('ECONNREFUSED');

    const app = createApp();
    const res = await request(app).get('/test');

    expect(res.status).toBe(503);
    expect(res.body.error).toBe('errors.session_store_unavailable');
    expect((app as any).errorCapture).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 503,
        message: 'errors.session_store_unavailable'
      })
    );
  });

  test('forwards error as-is when Redis is ready but store errors', async () => {
    fakeRedisClient.isReady = true;
    storeError = new Error('unexpected store error');

    const app = createApp();
    const res = await request(app).get('/test');

    expect(res.status).toBe(500);
    expect(res.body.error).toBe('unexpected store error');
    expect((app as any).errorCapture).not.toHaveBeenCalledWith(expect.objectContaining({ status: 503 }));
  });

  test('getSessionStoreStatus reports redis type with live isReady state', () => {
    fakeRedisClient.isReady = true;
    expect(getSessionStoreStatus()).toEqual({ type: 'redis', connected: true });

    fakeRedisClient.isReady = false;
    expect(getSessionStoreStatus()).toEqual({ type: 'redis', connected: false });
  });
});
