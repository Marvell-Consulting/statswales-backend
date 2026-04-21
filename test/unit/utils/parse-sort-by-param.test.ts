import { parseSortByParam, parseSortByToObjects } from '../../../src/utils/parse-sort-by-param';
import { BadRequestException } from '../../../src/exceptions/bad-request.exception';

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

  it('should trim whitespace around segments', () => {
    expect(parseSortByParam('title:asc, age:desc')).toEqual(['title|asc', 'age|desc']);
    expect(parseSortByParam(' title:asc , age:desc ')).toEqual(['title|asc', 'age|desc']);
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

  it('should throw BadRequestException for JSON with missing columnName', () => {
    const json = JSON.stringify([{ direction: 'ASC' }]);
    expect(() => parseSortByParam(json)).toThrow(BadRequestException);
  });

  it('should throw BadRequestException for JSON with empty columnName', () => {
    const json = JSON.stringify([{ columnName: '', direction: 'ASC' }]);
    expect(() => parseSortByParam(json)).toThrow(BadRequestException);
  });

  it('should throw BadRequestException for JSON with whitespace-only columnName', () => {
    const json = JSON.stringify([{ columnName: '   ', direction: 'ASC' }]);
    expect(() => parseSortByParam(json)).toThrow(BadRequestException);
  });

  it('should throw BadRequestException for JSON with invalid direction', () => {
    const json = JSON.stringify([{ columnName: 'title', direction: 'UP' }]);
    expect(() => parseSortByParam(json)).toThrow(BadRequestException);
  });

  it('should trim whitespace from JSON columnName', () => {
    const json = JSON.stringify([{ columnName: '  title  ', direction: 'ASC' }]);
    expect(parseSortByParam(json)).toEqual(['title|asc']);
  });

  it('should throw BadRequestException for invalid direction', () => {
    expect(() => parseSortByParam('title:up')).toThrow(BadRequestException);
  });
});

describe('parseSortByToObjects', () => {
  it('should return undefined for undefined input', () => {
    expect(parseSortByToObjects(undefined)).toBeUndefined();
  });

  it('should return undefined for empty string', () => {
    expect(parseSortByToObjects('')).toBeUndefined();
  });

  it('should parse single column to SortByInterface', () => {
    expect(parseSortByToObjects('title:asc')).toEqual([{ columnName: 'title', direction: 'ASC' }]);
  });

  it('should parse multiple columns to SortByInterface array', () => {
    expect(parseSortByToObjects('title:asc,age:desc')).toEqual([
      { columnName: 'title', direction: 'ASC' },
      { columnName: 'age', direction: 'DESC' }
    ]);
  });

  it('should default direction to ASC', () => {
    expect(parseSortByToObjects('title')).toEqual([{ columnName: 'title', direction: 'ASC' }]);
  });

  it('should parse legacy JSON format to SortByInterface', () => {
    const json = JSON.stringify([{ columnName: 'title', direction: 'DESC' }]);
    expect(parseSortByToObjects(json)).toEqual([{ columnName: 'title', direction: 'DESC' }]);
  });
});
