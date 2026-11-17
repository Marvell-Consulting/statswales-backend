const mockWarn = jest.fn();

jest.mock('../../../src/utils/logger', () => ({
  logger: { warn: (...args: unknown[]) => mockWarn(...args), info: jest.fn(), error: jest.fn(), debug: jest.fn() }
}));

import {
  isConnectionError,
  isStartupComplete,
  markStartupComplete,
  retryWhileDatabaseUnreachable
} from '../../../src/utils/startup';

const withCode = (code: string, message = 'boom'): Error => Object.assign(new Error(message), { code });

describe('isConnectionError', () => {
  test.each(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH'])(
    'is true for the network error %s',
    (code) => {
      expect(isConnectionError(withCode(code))).toBe(true);
    }
  );

  test.each(['57P01', '57P02', '57P03', '08000', '08001', '08003', '08004', '08006'])(
    'is true for the Postgres connection error %s',
    (code) => {
      expect(isConnectionError(withCode(code))).toBe(true);
    }
  );

  test.each([
    'timeout expired',
    'Connection terminated due to connection timeout',
    'Connection terminated unexpectedly',
    'timeout exceeded when trying to connect',
    'server login has been failing, cached error: connect failed (server_login_retry)',
    'the database system is starting up',
    'the database system is shutting down'
  ])('is true for the message "%s"', (message) => {
    expect(isConnectionError(new Error(message))).toBe(true);
  });

  test('is true when the cause is a connection error', () => {
    expect(isConnectionError(new Error('wrapper', { cause: withCode('ECONNREFUSED') }))).toBe(true);
  });

  test('is true when a TypeORM driverError is a connection error', () => {
    expect(isConnectionError(Object.assign(new Error('query failed'), { driverError: withCode('57P01') }))).toBe(true);
  });

  test('is true when any error in an AggregateError is a connection error', () => {
    expect(isConnectionError(new AggregateError([new Error('x'), withCode('ECONNREFUSED')]))).toBe(true);
  });

  test.each([
    ['a syntax error', withCode('42601', 'syntax error at or near "SELEC"')],
    ['a missing table', withCode('42P01', 'relation "foo" does not exist')],
    ['a failed login', withCode('28P01', 'password authentication failed for user "x"')],
    ['a statement timeout', withCode('57014', 'canceling statement due to statement timeout')],
    ['a plain error', new Error('something else')]
  ])('is false for %s', (_name, err) => {
    expect(isConnectionError(err)).toBe(false);
  });

  test.each([undefined, null, 'ECONNREFUSED', 42])('is false for %p', (value) => {
    expect(isConnectionError(value)).toBe(false);
  });

  test('does not loop on an error that is its own cause', () => {
    const err = new Error('loop') as Error & { cause?: unknown };
    err.cause = err;
    expect(isConnectionError(err)).toBe(false);
  });
});

describe('retryWhileDatabaseUnreachable', () => {
  const sleep = jest.fn().mockResolvedValue(undefined);

  beforeEach(() => {
    sleep.mockClear();
    mockWarn.mockClear();
  });

  test('returns the result when the task succeeds first time, without waiting', async () => {
    const task = jest.fn().mockResolvedValue('ok');

    await expect(retryWhileDatabaseUnreachable(task, { sleep })).resolves.toBe('ok');
    expect(task).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  test('retries connection errors with doubling delays up to the maximum', async () => {
    const task = jest
      .fn()
      .mockRejectedValueOnce(withCode('ECONNREFUSED'))
      .mockRejectedValueOnce(withCode('57P03'))
      .mockRejectedValueOnce(new Error('timeout expired'))
      .mockRejectedValueOnce(withCode('ECONNREFUSED'))
      .mockResolvedValue('ok');

    await expect(retryWhileDatabaseUnreachable(task, { sleep, initialDelayMs: 100, maxDelayMs: 300 })).resolves.toBe(
      'ok'
    );
    expect(task).toHaveBeenCalledTimes(5);
    expect(sleep.mock.calls.map(([ms]) => ms)).toEqual([100, 200, 300, 300]);
  });

  test('logs each failed attempt with the attempt number and the wait', async () => {
    const err = withCode('ECONNREFUSED');
    const task = jest.fn().mockRejectedValueOnce(err).mockResolvedValue('ok');

    await retryWhileDatabaseUnreachable(task, { sleep, initialDelayMs: 100 });

    expect(mockWarn).toHaveBeenCalledTimes(1);
    expect(mockWarn).toHaveBeenCalledWith({ err, attempt: 1, retryInMs: 100 }, expect.any(String));
  });

  test('rethrows other errors at once, without retrying', async () => {
    const err = withCode('42601', 'syntax error');
    const task = jest.fn().mockRejectedValue(err);

    await expect(retryWhileDatabaseUnreachable(task, { sleep })).rejects.toBe(err);
    expect(task).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  test('rethrows an error that comes after some connection errors', async () => {
    const err = withCode('28P01', 'password authentication failed');
    const task = jest.fn().mockRejectedValueOnce(withCode('ECONNREFUSED')).mockRejectedValue(err);

    await expect(retryWhileDatabaseUnreachable(task, { sleep })).rejects.toBe(err);
    expect(task).toHaveBeenCalledTimes(2);
  });
});

describe('startup state', () => {
  test('is not complete until marked complete', () => {
    expect(isStartupComplete()).toBe(false);
    markStartupComplete();
    expect(isStartupComplete()).toBe(true);
  });
});
