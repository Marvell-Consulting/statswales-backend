const mockConnect = jest.fn();
const mockQuery = jest.fn();
const mockEnd = jest.fn();

jest.mock('pg', () => ({
  Client: jest.fn().mockImplementation(() => ({
    connect: mockConnect,
    query: mockQuery,
    end: mockEnd
  }))
}));

jest.mock('../../../src/config', () => ({
  config: {
    database: {
      host: 'db-host',
      port: 5432,
      username: 'user',
      password: 'pw',
      database: 'sw',
      ssl: false
    },
    healthcheck: {
      dbTimeoutMs: 5000,
      storageTimeoutMs: 5000
    }
  }
}));

import { checkDb } from '../../../src/db/db-check';

describe('checkDb', () => {
  beforeEach(() => {
    mockConnect.mockReset();
    mockQuery.mockReset();
    mockEnd.mockReset().mockResolvedValue(undefined);
  });

  test('returns true when connect and query succeed', async () => {
    mockConnect.mockResolvedValue(undefined);
    mockQuery.mockResolvedValue({ rows: [{ connected: 1 }] });

    await expect(checkDb()).resolves.toBe(true);
    expect(mockConnect).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenCalledWith('SELECT 1 AS connected');
    expect(mockEnd).toHaveBeenCalledTimes(1);
  });

  test('rejects and still closes the client when connect fails', async () => {
    mockConnect.mockRejectedValue(new Error('ECONNREFUSED'));

    await expect(checkDb()).rejects.toThrow('ECONNREFUSED');
    expect(mockQuery).not.toHaveBeenCalled();
    expect(mockEnd).toHaveBeenCalledTimes(1);
  });

  test('rejects and still closes the client when query fails', async () => {
    mockConnect.mockResolvedValue(undefined);
    mockQuery.mockRejectedValue(new Error('query timeout'));

    await expect(checkDb()).rejects.toThrow('query timeout');
    expect(mockEnd).toHaveBeenCalledTimes(1);
  });

  test('swallows errors thrown while closing the client', async () => {
    mockConnect.mockResolvedValue(undefined);
    mockQuery.mockResolvedValue({ rows: [{ connected: 1 }] });
    mockEnd.mockRejectedValue(new Error('end failed'));

    await expect(checkDb()).resolves.toBe(true);
  });
});
