import { parseSortByParam } from '../../src/utils/parse-sort-by-param';
import { BadRequestException } from '../../src/exceptions/bad-request.exception';

describe('parseSortByParam', () => {
  it('should return empty array for undefined input', () => {
    expect(parseSortByParam(undefined)).toEqual([]);
  });

  it('should return empty array for empty string', () => {
    expect(parseSortByParam('')).toEqual([]);
  });

  it('should parse single column with direction', () => {
    expect(parseSortByParam('title:asc')).toEqual(['title|asc']);
  });

  it('should parse multiple columns', () => {
    expect(parseSortByParam('title:asc,age:desc')).toEqual(['title|asc', 'age|desc']);
  });

  it('should default direction to asc when omitted', () => {
    expect(parseSortByParam('title')).toEqual(['title|asc']);
  });

  it('should be case insensitive for direction', () => {
    expect(parseSortByParam('title:DESC')).toEqual(['title|desc']);
    expect(parseSortByParam('title:Asc')).toEqual(['title|asc']);
  });

  it('should parse legacy JSON array format', () => {
    const json = JSON.stringify([{ columnName: 'title', direction: 'ASC' }]);
    expect(parseSortByParam(json)).toEqual(['title|asc']);
  });

  it('should parse legacy JSON array with multiple items', () => {
    const json = JSON.stringify([
      { columnName: 'title', direction: 'ASC' },
      { columnName: 'age', direction: 'DESC' }
    ]);
    expect(parseSortByParam(json)).toEqual(['title|asc', 'age|desc']);
  });

  it('should parse legacy JSON array without direction (defaults to asc)', () => {
    const json = JSON.stringify([{ columnName: 'title' }]);
    expect(parseSortByParam(json)).toEqual(['title|asc']);
  });

  it('should throw BadRequestException for invalid JSON', () => {
    expect(() => parseSortByParam('[invalid')).toThrow(BadRequestException);
  });

  it('should throw BadRequestException for invalid direction', () => {
    expect(() => parseSortByParam('title:up')).toThrow(BadRequestException);
  });
});
