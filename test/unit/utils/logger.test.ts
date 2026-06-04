jest.mock('../../../src/config', () => ({
  config: {
    logger: { level: 'silent' }
  }
}));

import { serializeRequest } from '../../../src/utils/logger';

describe('serializeRequest (pino-http req serializer)', () => {
  const baseReq = {
    method: 'GET',
    url: '/v1/datasets?page=1',
    query: { page: '1' },
    params: { id: 'abc' },
    ip: '203.0.113.47'
  };

  test('emits only the picked fields plus the rateLimitBypass boolean', () => {
    expect(serializeRequest({ ...baseReq, rateLimitBypass: true })).toEqual({
      method: 'GET',
      url: '/v1/datasets?page=1',
      query: { page: '1' },
      params: { id: 'abc' },
      ip: '203.0.113.47',
      rateLimitBypass: true
    });
  });

  test('defaults rateLimitBypass to false when the flag is absent', () => {
    const out = serializeRequest({ ...baseReq });
    expect(out.rateLimitBypass).toBe(false);
  });

  test('includes the Express-resolved client IP', () => {
    const out = serializeRequest({ ...baseReq });
    expect(out.ip).toBe('203.0.113.47');
  });

  test('omits ip when the request has none (e.g. socket-less mock contexts)', () => {
    const { ip: _ip, ...reqWithoutIp } = baseReq;
    const out = serializeRequest(reqWithoutIp);
    expect(out).not.toHaveProperty('ip');
  });

  test('does not include sensitive headers or body even if present on the request', () => {
    const headers: Record<string, string> = { authorization: 'Bearer abc' };
    headers['x-rate-limit-bypass'] = 'super-secret-token';

    const out = serializeRequest({
      ...baseReq,
      headers,
      body: { password: 'hunter2' },
      rateLimitBypass: true
    });

    const serialised = JSON.stringify(out);
    expect(serialised).not.toContain('super-secret-token');
    expect(serialised).not.toContain('Bearer');
    expect(serialised).not.toContain('hunter2');
    expect(out).not.toHaveProperty('headers');
    expect(out).not.toHaveProperty('body');
  });
});
