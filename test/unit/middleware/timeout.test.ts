import express, { Request, Response } from 'express';
import request from 'supertest';

import { requestTimeout } from '../../../src/middleware/timeout';

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

function createApp(...middlewares: express.RequestHandler[]) {
  const app = express();
  for (const mw of middlewares) {
    app.use(mw);
  }
  return app;
}

describe('requestTimeout middleware', () => {
  describe('basic timeout behaviour', () => {
    test('returns 504 when the handler exceeds the timeout', async () => {
      const app = createApp(requestTimeout(50));
      app.get('/slow', (_req: Request, _res: Response) => {
        // never responds — the timeout should fire
      });

      const res = await request(app).get('/slow');
      expect(res.status).toBe(504);
      expect(res.body).toEqual({ message: 'Request timed out' });
    });

    test('returns normally when the handler responds before the timeout', async () => {
      const app = createApp(requestTimeout(500));
      app.get('/fast', (_req: Request, res: Response) => {
        res.status(200).json({ message: 'ok' });
      });

      const res = await request(app).get('/fast');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ message: 'ok' });
    });

    test('does not send a response if headers have already been sent', async () => {
      const app = createApp(requestTimeout(50));
      app.get('/partial', (_req: Request, res: Response) => {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        // headers sent but response not ended — timeout fires but should not crash
        setTimeout(() => res.end('done'), 100);
      });

      const res = await request(app).get('/partial');
      expect(res.status).toBe(200);
    });
  });

  describe('override behaviour', () => {
    test('a second timeout middleware cancels the first and uses its own duration', async () => {
      const shortTimeout = requestTimeout(50);
      const longTimeout = requestTimeout(2000);

      const app = createApp(shortTimeout);
      // The long timeout overrides the short one on this route
      app.get('/upload', longTimeout, (_req: Request, res: Response) => {
        setTimeout(() => res.status(200).json({ message: 'uploaded' }), 100);
      });

      const res = await request(app).get('/upload');
      // Without the override this would be 504 (100ms > 50ms)
      // With the override it succeeds (100ms < 2000ms)
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ message: 'uploaded' });
    });

    test('requestTimeout(0) disables the timeout entirely', async () => {
      const shortTimeout = requestTimeout(50);
      const disableTimeout = requestTimeout(0);

      const app = createApp(shortTimeout);
      app.get('/unlimited', disableTimeout, (_req: Request, res: Response) => {
        // Responds after 100ms — would normally exceed the 50ms global timeout
        setTimeout(() => res.status(200).json({ message: 'no limit' }), 100);
      });

      const res = await request(app).get('/unlimited');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ message: 'no limit' });
    });

    test('the overridden timeout still fires if exceeded', async () => {
      const shortTimeout = requestTimeout(2000);
      const overrideTimeout = requestTimeout(50);

      const app = createApp(shortTimeout);
      app.get('/capped', overrideTimeout, (_req: Request, _res: Response) => {
        // never responds
      });

      const res = await request(app).get('/capped');
      expect(res.status).toBe(504);
    });
  });
});
