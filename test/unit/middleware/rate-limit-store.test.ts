import { Options } from 'express-rate-limit';

import { RedisRateLimitStore } from '../../../src/middleware/rate-limit-store';

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

const exec = jest.fn();
const multi = {
  incr: jest.fn().mockReturnThis(),
  pExpire: jest.fn().mockReturnThis(),
  pTTL: jest.fn().mockReturnThis(),
  exec
};

const fakeClient = {
  isReady: true,
  multi: jest.fn(() => multi),
  decr: jest.fn().mockResolvedValue(0),
  del: jest.fn().mockResolvedValue(1)
};

const createStore = (): RedisRateLimitStore => {
  const store = new RedisRateLimitStore(fakeClient as any);
  store.init({ windowMs: 60000 } as Options);
  return store;
};

describe('RedisRateLimitStore', () => {
  let store: RedisRateLimitStore;

  beforeEach(() => {
    jest.clearAllMocks();
    fakeClient.isReady = true;
    store = createStore();
  });

  afterEach(() => store.shutdown());

  test('increments a prefixed key in redis and only sets expiry on the first hit of the window', async () => {
    exec.mockResolvedValue([3, 0, 45000]);

    const before = Date.now();
    const result = await store.increment('1.2.3.4');

    expect(multi.incr).toHaveBeenCalledWith('sw3b:rl:1.2.3.4');
    expect(multi.pExpire).toHaveBeenCalledWith('sw3b:rl:1.2.3.4', 60000, 'NX');
    expect(multi.pTTL).toHaveBeenCalledWith('sw3b:rl:1.2.3.4');
    expect(result.totalHits).toBe(3);
    expect(result.resetTime!.getTime()).toBeGreaterThanOrEqual(before + 45000);
  });

  test('falls back to in-memory counting when redis is not ready', async () => {
    fakeClient.isReady = false;

    expect((await store.increment('1.2.3.4')).totalHits).toBe(1);
    expect((await store.increment('1.2.3.4')).totalHits).toBe(2);
    expect(fakeClient.multi).not.toHaveBeenCalled();
  });

  test('falls back to in-memory counting when a redis command fails', async () => {
    exec.mockRejectedValue(new Error('connection lost'));

    const result = await store.increment('1.2.3.4');

    expect(result.totalHits).toBe(1);
  });

  test('resetKey clears both redis and the fallback store', async () => {
    fakeClient.isReady = false;
    await store.increment('1.2.3.4');
    fakeClient.isReady = true;

    await store.resetKey('1.2.3.4');
    fakeClient.isReady = false;

    expect(fakeClient.del).toHaveBeenCalledWith('sw3b:rl:1.2.3.4');
    expect((await store.increment('1.2.3.4')).totalHits).toBe(1);
  });
});
