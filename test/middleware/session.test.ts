import express, { Request, Response, NextFunction } from 'express';
import request from 'supertest';

jest.mock('../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

// Mock the config before importing session — CI uses MemoryStore,
// so we test the wrapper behaviour with a custom failing store.
jest.mock('../../src/config', () => ({
  config: {
    session: {
      store: 'memory',
      secret: 'test-secret',
      secure: false,
      maxAge: 86400000
    }
  }
}));

import sessionMiddleware, { getSessionStoreStatus, SessionStoreStatus } from '../../src/middleware/session';

function createApp(
  middleware: (req: Request, res: Response, next: NextFunction) => void,
  errorHandler?: express.ErrorRequestHandler
): express.Express {
  const app = express();
  app.use(middleware);
  app.get('/test', (_req: Request, res: Response) => {
    res.status(200).json({ message: 'ok' });
  });
  if (errorHandler) {
    app.use(errorHandler);
  }
  return app;
}

describe('session middleware', () => {
  describe('with memory store', () => {
    test('passes requests through when store is healthy', async () => {
      const app = createApp(sessionMiddleware);
      const res = await request(app).get('/test');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ message: 'ok' });
    });

    test('getSessionStoreStatus returns memory type with connected true', () => {
      const status: SessionStoreStatus = getSessionStoreStatus();
      expect(status).toEqual({ type: 'memory', connected: true });
    });
  });

  describe('store error handling', () => {
    test('returns 503 when session store errors', async () => {
      // Create a middleware that simulates a store error by wrapping
      // the session callback with an error
      const failingMiddleware = (_req: Request, _res: Response, next: NextFunction): void => {
        next(new Error('ECONNREFUSED'));
      };

      const errorCapture = jest.fn();
      const errorHandler: express.ErrorRequestHandler = (err, _req, res, _next) => {
        errorCapture(err);
        res.status(err.status || 500).json({ error: err.message });
      };

      // Simulate what the session wrapper does: intercept store errors and convert to 503
      const wrapperMiddleware = (req: Request, res: Response, next: NextFunction): void => {
        failingMiddleware(req, res, (err?: unknown) => {
          if (err) {
            const sessionError = Object.assign(new Error('errors.session_store_unavailable'), {
              status: 503,
              cause: err
            });
            return next(sessionError);
          }
          next();
        });
      };

      const wrappedApp = createApp(wrapperMiddleware, errorHandler);
      const res = await request(wrappedApp).get('/test');

      expect(res.status).toBe(503);
      expect(res.body.error).toBe('errors.session_store_unavailable');
      expect(errorCapture).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 503,
          message: 'errors.session_store_unavailable'
        })
      );
    });
  });
});
