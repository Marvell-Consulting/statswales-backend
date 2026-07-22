jest.mock('../../../src/utils/logger', () => ({
  logger: {
    info: jest.fn(),
    error: jest.fn()
  }
}));

import { withAdvisoryLock } from '../../../src/utils/advisory-lock';

describe('withAdvisoryLock', () => {
  const makeDataSource = (query: jest.Mock, release: jest.Mock) => ({
    createQueryRunner: jest.fn(() => ({ query, release }))
  });

  it('runs the callback and releases the lock when it acquires the lock', async () => {
    const release = jest.fn();
    const query = jest
      .fn()
      .mockResolvedValueOnce([{ locked: true }]) // pg_try_advisory_lock
      .mockResolvedValueOnce(undefined); // pg_advisory_unlock
    const dataSource = makeDataSource(query, release) as any;
    const fn = jest.fn().mockResolvedValue('result');

    const result = await withAdvisoryLock(dataSource, 123, fn);

    expect(result).toBe('result');
    expect(fn).toHaveBeenCalledTimes(1);
    expect(query).toHaveBeenNthCalledWith(1, 'SELECT pg_try_advisory_lock($1) AS locked', [123]);
    expect(query).toHaveBeenNthCalledWith(2, 'SELECT pg_advisory_unlock($1)', [123]);
    expect(release).toHaveBeenCalledTimes(1);
  });

  it('skips the callback and returns undefined when the lock is held elsewhere', async () => {
    const release = jest.fn();
    const query = jest.fn().mockResolvedValueOnce([{ locked: false }]);
    const dataSource = makeDataSource(query, release) as any;
    const fn = jest.fn();

    const result = await withAdvisoryLock(dataSource, 123, fn);

    expect(result).toBeUndefined();
    expect(fn).not.toHaveBeenCalled();
    expect(query).toHaveBeenCalledTimes(1);
    expect(release).toHaveBeenCalledTimes(1);
  });

  it('still releases the lock and the connection when the callback throws', async () => {
    const release = jest.fn();
    const query = jest
      .fn()
      .mockResolvedValueOnce([{ locked: true }])
      .mockResolvedValueOnce(undefined);
    const dataSource = makeDataSource(query, release) as any;
    const fn = jest.fn().mockRejectedValue(new Error('boom'));

    await expect(withAdvisoryLock(dataSource, 123, fn)).rejects.toThrow('boom');

    expect(query).toHaveBeenNthCalledWith(2, 'SELECT pg_advisory_unlock($1)', [123]);
    expect(release).toHaveBeenCalledTimes(1);
  });
});
