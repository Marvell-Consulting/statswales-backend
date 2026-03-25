import { Request } from 'express';
import { validationResult } from 'express-validator';

import { pageSizeValidator } from '../../src/validators';
import { MAX_PAGE_SIZE } from '../../src/utils/page-defaults';

function mockRequest(query: Partial<Record<string, string>>): Partial<Request> {
  return { query } as Partial<Request>;
}

async function validate(query: Partial<Record<string, string>>) {
  const req = mockRequest(query);
  await pageSizeValidator().run(req as Request);
  return validationResult(req as Request);
}

describe('pageSizeValidator', () => {
  it('should accept a valid page_size', async () => {
    const result = await validate({ page_size: '50' });
    expect(result.isEmpty()).toBe(true);
  });

  it('should accept page_size at the maximum', async () => {
    const result = await validate({ page_size: String(MAX_PAGE_SIZE) });
    expect(result.isEmpty()).toBe(true);
  });

  it('should accept page_size of 1', async () => {
    const result = await validate({ page_size: '1' });
    expect(result.isEmpty()).toBe(true);
  });

  it('should reject page_size above the maximum', async () => {
    const result = await validate({ page_size: String(MAX_PAGE_SIZE + 1) });
    expect(result.isEmpty()).toBe(false);
  });

  it('should reject page_size of 0', async () => {
    const result = await validate({ page_size: '0' });
    expect(result.isEmpty()).toBe(false);
  });

  it('should reject negative page_size', async () => {
    const result = await validate({ page_size: '-1' });
    expect(result.isEmpty()).toBe(false);
  });

  it('should pass when page_size is not provided', async () => {
    const result = await validate({});
    expect(result.isEmpty()).toBe(true);
  });
});
