import express, { Request, Response } from 'express';
import request from 'supertest';

const mockConfig = {
  session: { store: 'memory' },
  rateLimit: {
    windowMs: 60000,
    maxRequests: 2,
    bypassToken: undefined as string | undefined
  }
};

jest.mock('../../../src/config', () => ({
  config: mockConfig
}));

jest.mock('../../../src/utils/logger', () => ({
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
    rateLimiter = require('../../../src/middleware/rate-limiter').rateLimiter;
  });

  const app = express();
  app.use(rateLimiter!);
  app.get('/test', (req: Request, res: Response) => {
    res.status(200).json({ message: 'ok', rateLimitBypass: req.rateLimitBypass ?? false });
  });
  return app;
}

describe('rateLimiter middleware', () => {
  describe('bypass token', () => {
    beforeEach(() => {
      mockConfig.rateLimit.bypassToken = 'test-secret-token';
    });

    test('bypasses rate limit and flags req.rateLimitBypass=true when valid token header is present', async () => {
      const app = createApp();

      for (let i = 0; i < 5; i++) {
        const res = await request(app).get('/test').set('x-rate-limit-bypass', 'test-secret-token');
        expect(res.status).toBe(200);
        expect(res.body.rateLimitBypass).toBe(true);
      }
    });

    test('applies rate limit and leaves req.rateLimitBypass=false when no bypass header is present', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test');
        expect(res.status).toBe(200);
        expect(res.body.rateLimitBypass).toBe(false);
      }

      const res = await request(app).get('/test');
      expect(res.status).toBe(429);
      expect(res.body).toEqual({ message: 'Too many requests, please try again later.' });
    });

    test('applies rate limit and leaves req.rateLimitBypass=false when bypass header has wrong token', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test').set('x-rate-limit-bypass', 'wrong-token');
        expect(res.status).toBe(200);
        expect(res.body.rateLimitBypass).toBe(false);
      }

      const res = await request(app).get('/test').set('x-rate-limit-bypass', 'wrong-token');
      expect(res.status).toBe(429);
    });
  });

  describe('no bypass token configured', () => {
    beforeEach(() => {
      mockConfig.rateLimit.bypassToken = undefined;
    });

    test('applies rate limit and leaves req.rateLimitBypass=false even when bypass header is present', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test').set('x-rate-limit-bypass', 'some-token');
        expect(res.status).toBe(200);
        expect(res.body.rateLimitBypass).toBe(false);
      }

      const res = await request(app).get('/test').set('x-rate-limit-bypass', 'some-token');
      expect(res.status).toBe(429);
    });
  });

  describe('client IP keying', () => {
    beforeEach(() => {
      mockConfig.rateLimit.bypassToken = undefined;
    });

    test('counts requests per x-azure-socketip rather than per proxy address', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app).get('/test').set('x-azure-socketip', '203.0.113.1');
        expect(res.status).toBe(200);
      }

      const limited = await request(app).get('/test').set('x-azure-socketip', '203.0.113.1');
      expect(limited.status).toBe(429);

      const otherClient = await request(app).get('/test').set('x-azure-socketip', '203.0.113.2');
      expect(otherClient.status).toBe(200);
    });

    test('ignores x-forwarded-for so clients cannot rotate their rate limit key', async () => {
      const app = createApp();

      for (let i = 0; i < 2; i++) {
        const res = await request(app)
          .get('/test')
          .set('x-azure-socketip', '203.0.113.1')
          .set('x-forwarded-for', `198.51.100.${i}`);
        expect(res.status).toBe(200);
      }

      const res = await request(app)
        .get('/test')
        .set('x-azure-socketip', '203.0.113.1')
        .set('x-forwarded-for', '198.51.100.99');
      expect(res.status).toBe(429);
    });
  });

  describe('getClientIp', () => {
    let getClientIp: (req: Request) => string;

    beforeAll(() => {
      jest.isolateModules(() => {
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        getClientIp = require('../../../src/middleware/rate-limiter').getClientIp;
      });
    });

    const fakeReq = (header: string | undefined, ip = '10.0.0.1'): Request =>
      ({ ip, get: (name: string) => (name === 'x-azure-socketip' ? header : undefined) }) as unknown as Request;

    test('prefers a valid x-azure-socketip header', () => {
      expect(getClientIp(fakeReq('203.0.113.1'))).toBe('203.0.113.1');
      expect(getClientIp(fakeReq('2001:db8::1'))).toBe('2001:db8::1');
    });

    test('falls back to req.ip when the header is missing or invalid', () => {
      expect(getClientIp(fakeReq(undefined))).toBe('10.0.0.1');
      expect(getClientIp(fakeReq('not-an-ip'))).toBe('10.0.0.1');
    });
  });
});
