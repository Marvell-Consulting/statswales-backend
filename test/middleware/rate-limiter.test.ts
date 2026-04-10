import express, { Request, Response } from 'express';
import request from 'supertest';

const mockConfig = {
  rateLimit: {
    windowMs: 60000,
    maxRequests: 2,
    bypassToken: undefined as string | undefined
  }
};

jest.mock('../../src/config', () => ({
  config: mockConfig
}));

jest.mock('../../src/utils/logger', () => ({
  logger: {
    trace: jest.fn(),
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

function createApp() {
  // Re-import the module each time to get a fresh rate limiter with a clean counter
  let rateLimiter: express.RequestHandler;
  jest.isolateModules(() => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    rateLimiter = require('../../src/middleware/rate-limiter').rateLimiter;
  });

  const app = express();
  app.use(rateLimiter!);
  app.get('/test', (_req: Request, res: Response) => {
    res.status(200).json({ message: 'ok' });
  });
  return app;
}

describe('rateLimiter middleware', () => {
  describe('bypass token', () => {
    beforeEach(() => {
      mockConfig.rateLimit.bypassToken = 'test-secret-token';
    });

    test('bypasses rate limit when valid token header is present', async () => {
      const app = createApp();

      for (let i = 0; i < 5; i++) {
        const res = await request(app).get('/test').set('x-rate-limit-bypass', 'test-secret-token');
        expect(res.status).toBe(200);
      }
    });

    test('applies rate limit when no bypass header is present', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test');
        expect(res.status).toBe(200);
      }

      const res = await request(app).get('/test');
      expect(res.status).toBe(429);
      expect(res.body).toEqual({ message: 'Too many requests, please try again later.' });
    });

    test('applies rate limit when bypass header has wrong token', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test').set('x-rate-limit-bypass', 'wrong-token');
        expect(res.status).toBe(200);
      }

      const res = await request(app).get('/test').set('x-rate-limit-bypass', 'wrong-token');
      expect(res.status).toBe(429);
    });
  });

  describe('no bypass token configured', () => {
    beforeEach(() => {
      mockConfig.rateLimit.bypassToken = undefined;
    });

    test('applies rate limit even when bypass header is present', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test').set('x-rate-limit-bypass', 'some-token');
        expect(res.status).toBe(200);
      }

      const res = await request(app).get('/test').set('x-rate-limit-bypass', 'some-token');
      expect(res.status).toBe(429);
    });
  });
});
