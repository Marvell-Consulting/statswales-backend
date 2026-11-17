const mockWarn = jest.fn();

jest.mock('../../../src/utils/logger', () => ({
  logger: { warn: (...args: unknown[]) => mockWarn(...args), info: jest.fn(), error: jest.fn(), debug: jest.fn() }
}));

import {
  isConnectionError,
  isStartupComplete,
  markStartupComplete,
  markStartupPending,
  pickStartupError,
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

  test.each(['57P01', '57P02', '57P03', '08000', '08001', '08003', '08004', '08006', '08007'])(
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
    'the database system is shutting down',
    'no more connections allowed (max_client_conn)'
  ])('is true for the message "%s"', (message) => {
    expect(isConnectionError(new Error(message))).toBe(true);
  });

  test('is true when the cause is a connection error', () => {
    expect(isConnectionError(new Error('wrapper', { cause: withCode('ECONNREFUSED') }))).toBe(true);
  });

  test('is true when a TypeORM driverError is a connection error', () => {
    expect(isConnectionError(Object.assign(new Error('query failed'), { driverError: withCode('57P01') }))).toBe(true);
  });

  test('is true when every error in an AggregateError is a connection error', () => {
    expect(isConnectionError(new AggregateError([withCode('ECONNREFUSED'), withCode('ETIMEDOUT')]))).toBe(true);
  });

  test('is false when an AggregateError also holds an error that retrying will not fix', () => {
    const login = withCode('28P01', 'password authentication failed');
    expect(isConnectionError(new AggregateError([withCode('ECONNREFUSED'), login]))).toBe(false);
  });

  test('is false for an empty AggregateError', () => {
    expect(isConnectionError(new AggregateError([]))).toBe(false);
  });

  test('is false for a message that only contains "timeout expired"', () => {
    expect(isConnectionError(new Error('lock timeout expired while running migration'))).toBe(false);
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

describe('pickStartupError', () => {
  test('picks an error that is not a connection error, so startup fails at once', () => {
    const login = withCode('28P01', 'password authentication failed');
    expect(pickStartupError([withCode('ECONNREFUSED'), login, withCode('57P03')])).toBe(login);
  });

  test('picks the first error when they are all connection errors, so startup retries', () => {
    const first = withCode('ECONNREFUSED');
    expect(pickStartupError([first, withCode('57P03')])).toBe(first);
  });
});

describe('startup state', () => {
  // The server marks startup pending before it listens. Anything that imports the app without the server, such as
  // the tests, sees it as complete.
  test('is complete unless the server marks it pending, until it is marked complete', () => {
    expect(isStartupComplete()).toBe(true);
    markStartupPending();
    expect(isStartupComplete()).toBe(false);
    markStartupComplete();
    expect(isStartupComplete()).toBe(true);
  });
});
