/* eslint-disable @typescript-eslint/naming-convention */

import { PassThrough } from 'node:stream';
import { Response } from 'express';

// Mock logger before other imports
jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    trace: jest.fn()
  }
}));

// Mock DuckDB
const mockDuckdbRun = jest.fn();
const mockDuckdbStream = jest.fn();
const mockReleaseDuckDB = jest.fn();
jest.mock('../../../src/services/duckdb', () => ({
  acquireDuckDB: jest.fn().mockImplementation(async () => ({
    duckdb: { run: mockDuckdbRun, stream: mockDuckdbStream },
    releaseDuckDB: mockReleaseDuckDB
  }))
}));

// Mock database manager
const mockCubeQuery = jest.fn();
const mockQueryRunnerQuery = jest.fn();
const mockQueryRunnerRelease = jest.fn();
jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: () => ({
      query: mockCubeQuery,
      createQueryRunner: () => ({
        query: mockQueryRunnerQuery,
        release: mockQueryRunnerRelease
      })
    })
  }
}));

// Mock translation
const mockT = jest.fn().mockImplementation((key: string) => {
  if (key === 'column_headers.data_values') return 'Data values';
  return key;
});
jest.mock('../../../src/middleware/translation', () => ({
  t: (...args: any[]) => mockT(...args)
}));

// Mock DatasetRepository
const mockGetById = jest.fn();
jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: (...args: unknown[]) => mockGetById(...args)
  }
}));

// Mock consumer utils
const mockGetFilterTable = jest.fn();
const mockResolveDimensionToFactTableColumn = jest.fn();
const mockResolveFactColumnToDimension = jest.fn();
jest.mock('../../../src/utils/consumer', () => ({
  getFilterTable: (...args: unknown[]) => mockGetFilterTable(...args),
  resolveDimensionToFactTableColumn: (...args: unknown[]) => mockResolveDimensionToFactTableColumn(...args),
  resolveFactColumnToDimension: (...args: unknown[]) => mockResolveFactColumnToDimension(...args)
}));

// Mock column headers
jest.mock('../../../src/utils/column-headers', () => ({
  getColumnHeaders: jest.fn().mockReturnValue([{ index: 0, name: 'Col1', source_type: 'dimension' }])
}));

// Mock ConsumerDatasetDTO
jest.mock('../../../src/dtos/consumer-dataset-dto', () => ({
  ConsumerDatasetDTO: {
    fromDataset: jest.fn().mockReturnValue({ id: 'dataset-id', title: 'Test Dataset' })
  }
}));

// Mock cube-builder
jest.mock('../../../src/services/cube-builder', () => ({
  makeCubeSafeString: jest.fn((str: string) =>
    str
      .toLowerCase()
      .replace(/[ ]/g, '_')
      .replace(/[^a-zA-Z_]/g, '')
  )
}));

import ExcelJS from 'exceljs';
import { DuckDBValue } from '@duckdb/node-api';
import { QueryStore } from '../../../src/entities/query-store';
import { PageOptions } from '../../../src/interfaces/page-options';
import { Locale } from '../../../src/enums/locale';
import { OutputFormats } from '../../../src/enums/output-formats';
import { DimensionType } from '../../../src/enums/dimension-type';
import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import {
  langToLocale,
  createPivotQuery,
  createPivotOutputUsingDuckDB,
  getSortedPivotColumns
} from '../../../src/services/pivots';

// Helper to create a mock QueryStore
function createMockQueryStore(overrides: Partial<QueryStore> = {}): QueryStore {
  return {
    id: 'test-query-store-id',
    hash: 'test-hash',
    datasetId: 'test-dataset-id',
    revisionId: 'test-revision-id',
    requestObject: { filters: [], pivot: {} },
    query: { en: 'SELECT * FROM "test-revision-id"."view"', cy: 'SELECT * FROM "test-revision-id"."view"' },
    totalLines: 100,
    totalPivotLines: 50,
    columnMapping: [],
    createdAt: new Date(),
    updatedAt: new Date(),
    ...overrides
  } as QueryStore;
}

// Helper to create a mock DuckDBResult with yieldRows() and columnNames()
function createMockDuckDBResult(
  columns: string[],
  rows: DuckDBValue[][]
): {
  columnNames: () => string[];
  yieldRows: () => AsyncIterableIterator<DuckDBValue[][]>;
} {
  return {
    columnNames: () => columns,
    async *yieldRows() {
      if (rows.length > 0) {
        yield rows;
      }
    }
  };
}

// Helper to create a mock Response that captures written data
function createMockStreamResponse(): Response & { writtenData: string[] } {
  const writtenData: string[] = [];
  const stream = new PassThrough();

  const originalWrite = stream.write.bind(stream);
  stream.write = ((chunk: any, encodingOrCallback?: any, callback?: any) => {
    if (chunk) {
      writtenData.push(typeof chunk === 'string' ? chunk : chunk.toString());
    }
    if (typeof encodingOrCallback === 'function') {
      return originalWrite(chunk, encodingOrCallback);
    }
    return originalWrite(chunk, encodingOrCallback, callback);
  }) as typeof stream.write;

  const res = stream as unknown as Response & { writtenData: string[] };
  res.writtenData = writtenData;
  res.setHeader = jest.fn().mockReturnThis();
  res.writeHead = jest.fn().mockReturnThis();
  res.flushHeaders = jest.fn();
  res.status = jest.fn().mockReturnThis();
  res.json = jest.fn();
  (res as any).headersSent = false;

  return res;
}

// Helper to create a mock Response that captures binary data (for Excel tests)
function createMockBinaryResponse(): Response & { getBuffer: () => Promise<Buffer> } {
  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));

  const res = stream as unknown as Response & { getBuffer: () => Promise<Buffer> };
  res.setHeader = jest.fn().mockReturnThis();
  res.writeHead = jest.fn().mockReturnThis();
  res.flushHeaders = jest.fn();
  res.status = jest.fn().mockReturnThis();
  res.json = jest.fn();
  (res as any).headersSent = false;
  res.getBuffer = () =>
    new Promise<Buffer>((resolve) => {
      stream.on('end', () => resolve(Buffer.concat(chunks)));
      if (stream.readableEnded) resolve(Buffer.concat(chunks));
    });

  return res;
}

function defaultPageOptions(overrides: Partial<PageOptions> = {}): PageOptions {
  return {
    format: OutputFormats.Json,
    pageNumber: 1,
    pageSize: 100,
    sort: [],
    locale: Locale.EnglishGb,
    x: 'Period',
    y: 'Area',
    ...overrides
  };
}

describe('pivots service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockQueryRunnerRelease.mockResolvedValue(undefined);
  });

  describe('langToLocale', () => {
    it('returns en-GB for "en"', () => {
      expect(langToLocale('en')).toBe('en-GB');
    });

    it('returns cy-GB for "cy"', () => {
      expect(langToLocale('cy')).toBe('cy-GB');
    });

    it('returns en-GB for empty string', () => {
      expect(langToLocale('')).toBe('en-GB');
    });

    it('returns en-GB for unknown language', () => {
      expect(langToLocale('fr')).toBe('en-GB');
    });

    it('truncates 5-char locale to 2-char code', () => {
      expect(langToLocale('en-GB')).toBe('en-GB');
      expect(langToLocale('cy-GB')).toBe('cy-GB');
    });
  });

  describe('getSortedPivotColumns', () => {
    const queryStore = createMockQueryStore();

    beforeEach(() => {
      mockGetFilterTable.mockResolvedValue([
        { fact_table_column: 'period_col', dimension_name: 'Period', language: 'en' }
      ]);
      mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
      mockGetById.mockResolvedValue({
        factTable: [{ columnName: 'period_col' }],
        dimensions: [{ factTableColumn: 'period_col', type: DimensionType.DatePeriod }]
      });
    });

    it('returns original order when x is an array (multi-dimensional pivot)', async () => {
      const columns = ['Area', '2020', '2021', '2022'];
      const pageOptions = defaultPageOptions({ x: ['Period', 'Measure'] });

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
    });

    it('returns original order when x is undefined', async () => {
      const columns = ['Area', '2020', '2021'];
      const pageOptions = defaultPageOptions({ x: undefined });

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
    });

    it('returns original order when there is only one x-value column', async () => {
      const columns = ['Area', '2020'];
      const pageOptions = defaultPageOptions();

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
    });

    it('sorts date dimension columns in DESC order from lookup table', async () => {
      const columns = ['Area', '2020', '2022', '2021'];
      const pageOptions = defaultPageOptions();

      mockCubeQuery.mockResolvedValue([{ description: '2022' }, { description: '2021' }, { description: '2020' }]);

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(['Area', '2022', '2021', '2020']);
    });

    it('sorts non-date dimension columns in ASC order from lookup table', async () => {
      const columns = ['Period', 'Zebra', 'Aardvark', 'Mango'];
      const pageOptions = defaultPageOptions({ x: 'Fruit' });

      mockResolveDimensionToFactTableColumn.mockReturnValue('fruit_col');
      mockGetById.mockResolvedValue({
        factTable: [{ columnName: 'fruit_col' }],
        dimensions: [{ factTableColumn: 'fruit_col', type: DimensionType.LookupTable }]
      });
      mockCubeQuery.mockResolvedValue([
        { description: 'Aardvark' },
        { description: 'Mango' },
        { description: 'Zebra' }
      ]);

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(['Period', 'Aardvark', 'Mango', 'Zebra']);
    });

    it('preserves multiple y columns at the start', async () => {
      const columns = ['Area', 'Measure', '2022', '2020', '2021'];
      const pageOptions = defaultPageOptions({ y: ['Area', 'Measure'] });

      mockCubeQuery.mockResolvedValue([{ description: '2022' }, { description: '2021' }, { description: '2020' }]);

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(['Area', 'Measure', '2022', '2021', '2020']);
    });

    it('returns original order when dimension has no lookup table', async () => {
      const columns = ['Area', 'B', 'A', 'C'];
      const pageOptions = defaultPageOptions({ x: 'Value' });

      mockResolveDimensionToFactTableColumn.mockReturnValue('value_col');
      mockGetById.mockResolvedValue({
        factTable: [{ columnName: 'value_col' }],
        dimensions: [{ factTableColumn: 'value_col', type: DimensionType.Numeric }]
      });

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
      expect(mockCubeQuery).not.toHaveBeenCalled();
    });

    it('resolves x as raw fact table column name when dimension name lookup fails', async () => {
      const columns = ['Area', '2020', '2022', '2021'];
      const pageOptions = defaultPageOptions({ x: 'DateRef' });

      // resolveDimensionToFactTableColumn throws — x is a raw column name, not a dimension name
      mockResolveDimensionToFactTableColumn.mockImplementation(() => {
        throw new Error('Column not found');
      });
      // But the filter table has the fact_table_column
      mockGetFilterTable.mockResolvedValue([{ fact_table_column: 'DateRef', dimension_name: 'Date', language: 'en' }]);
      mockGetById.mockResolvedValue({
        factTable: [{ columnName: 'DateRef' }],
        dimensions: [{ factTableColumn: 'DateRef', type: DimensionType.Date }]
      });
      mockCubeQuery.mockResolvedValue([{ description: '2022' }, { description: '2021' }, { description: '2020' }]);

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(['Area', '2022', '2021', '2020']);
    });

    it('returns original order when x matches neither dimension name nor fact table column', async () => {
      const columns = ['Area', '2020', '2021'];
      const pageOptions = defaultPageOptions({ x: 'Unknown' });

      mockResolveDimensionToFactTableColumn.mockImplementation(() => {
        throw new Error('Column not found');
      });
      mockGetFilterTable.mockResolvedValue([{ fact_table_column: 'DateRef', dimension_name: 'Date', language: 'en' }]);

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
    });

    it('falls back to original order on query failure', async () => {
      const columns = ['Area', '2020', '2021'];
      const pageOptions = defaultPageOptions();

      mockCubeQuery.mockRejectedValue(new Error('DB connection failed'));

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
    });

    it('falls back to original order when lookup returns empty results', async () => {
      const columns = ['Area', '2020', '2021'];
      const pageOptions = defaultPageOptions();

      mockCubeQuery.mockResolvedValue([]);

      const result = await getSortedPivotColumns(columns, pageOptions, queryStore, 'en');

      expect(result).toEqual(columns);
    });
  });

  describe('createPivotQuery', () => {
    beforeEach(() => {
      mockGetFilterTable.mockResolvedValue([
        { fact_table_column: 'period_col', dimension_name: 'Period', language: 'en' },
        { fact_table_column: 'area_col', dimension_name: 'Area', language: 'en' }
      ]);
      mockResolveDimensionToFactTableColumn.mockImplementation((name: string) => name);
    });

    it('generates a PIVOT query with x and y', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions();

      const result = await createPivotQuery('en', queryStore, pageOptions);

      expect(result).toContain('PIVOT');
      expect(result).toContain('GROUP BY');
      expect(result).toContain('"Period"');
      expect(result).toContain('"Area"');
    });

    it('throws when x is missing', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions({ x: undefined });

      await expect(createPivotQuery('en', queryStore, pageOptions)).rejects.toThrow(BadRequestException);
    });

    it('throws when y is missing', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions({ y: undefined });

      await expect(createPivotQuery('en', queryStore, pageOptions)).rejects.toThrow(BadRequestException);
    });

    it('includes LIMIT and OFFSET when pageSize is set', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions({ pageSize: 50, pageNumber: 3 });

      const result = await createPivotQuery('en', queryStore, pageOptions);

      expect(result).toContain('LIMIT 50');
      expect(result).toContain('OFFSET 100');
    });

    it('handles array x values with concatenation', async () => {
      const queryStore = createMockQueryStore({
        query: {
          en: 'SELECT "Period", "Measure" FROM "test-revision-id"."view"',
          cy: 'SELECT "Period", "Measure" FROM "test-revision-id"."view"'
        }
      });
      const pageOptions = defaultPageOptions({ x: ['Period', 'Measure'] });

      const result = await createPivotQuery('en', queryStore, pageOptions);

      expect(result).toContain("|| ' & ' ||");
    });

    it('handles array y values', async () => {
      const queryStore = createMockQueryStore({
        query: {
          en: 'SELECT "Area", "Period" FROM "test-revision-id"."view"',
          cy: 'SELECT "Area", "Period" FROM "test-revision-id"."view"'
        }
      });
      const pageOptions = defaultPageOptions({ y: ['Area', 'Period'] });

      const result = await createPivotQuery('en', queryStore, pageOptions);

      expect(result).toContain('"Area", "Period"');
    });

    it('includes ORDER BY when sort is specified', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions({ sort: ['Area|ASC'] });

      const result = await createPivotQuery('en', queryStore, pageOptions);

      expect(result).toContain('ORDER BY');
      expect(result).toContain('"Area" ASC');
    });

    it('throws on invalid sort direction', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions({ sort: ['Area|INVALID'] });

      await expect(createPivotQuery('en', queryStore, pageOptions)).rejects.toThrow(BadRequestException);
    });

    it('throws when x value is not present in the query', async () => {
      const queryStore = createMockQueryStore();
      const pageOptions = defaultPageOptions({ x: ['NotInQuery'] });

      await expect(createPivotQuery('en', queryStore, pageOptions)).rejects.toThrow(BadRequestException);
    });
  });

  describe('createPivotOutputUsingDuckDB', () => {
    function setupMockDuckDB(columns: string[], rows: DuckDBValue[][]) {
      const mockResult = createMockDuckDBResult(columns, rows);
      mockDuckdbRun.mockResolvedValue({ rowCount: rows.length });
      mockDuckdbStream.mockResolvedValue(mockResult);
    }

    describe('JSON output', () => {
      it('produces valid JSON with all data', async () => {
        const columns = ['Area', '2020', '2021', '2022'];
        const rows: DuckDBValue[][] = [
          ['Cardiff', 100, 200, 300],
          ['Swansea', 150, 250, 350]
        ];
        setupMockDuckDB(columns, rows);

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Json });

        mockGetFilterTable.mockResolvedValue([]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        const parsed = JSON.parse(output);

        expect(parsed.pivot).toHaveLength(2);
        expect(parsed.pivot[0]).toEqual({ Area: 'Cardiff', '2020': 100, '2021': 200, '2022': 300 });
        expect(parsed.pivot[1]).toEqual({ Area: 'Swansea', '2020': 150, '2021': 250, '2022': 350 });
      });

      it('preserves column order even with integer-like keys', async () => {
        // This is the core bug fix — integer-like keys would be reordered by Object.keys()
        // after JSON.parse, so we verify the raw serialised output string order instead.
        const columns = ['Region', '2020', '2021', '100'];
        const rows: DuckDBValue[][] = [['Wales', 'a', 'b', 'c']];
        setupMockDuckDB(columns, rows);

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Json });

        mockGetFilterTable.mockResolvedValue([]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        // The raw JSON keys must appear in DuckDB's column order, not V8's integer-first order.
        // Extract the first row object from within the pivot array.
        const pivotArrayMatch = output.match(/\[\{([^}]+)\}/);
        expect(pivotArrayMatch).not.toBeNull();
        const keyOrder = [...pivotArrayMatch![0].matchAll(/"([^"]+)":/g)].map((m) => m[1]);
        expect(keyOrder).toEqual(['Region', '2020', '2021', '100']);
      });
    });

    describe('CSV output', () => {
      it('writes CSV with headers in correct order', async () => {
        const columns = ['Area', '2020', '2021'];
        const rows: DuckDBValue[][] = [['Cardiff', 100, 200]];
        setupMockDuckDB(columns, rows);

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Csv });

        mockGetFilterTable.mockResolvedValue([]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        const lines = output.trim().split('\n');

        expect(lines[0]).toBe('Area,2020,2021');
        expect(lines[1]).toBe('Cardiff,100,200');
      });
    });

    describe('HTML output', () => {
      it('writes HTML table with headers in correct order', async () => {
        const columns = ['Area', '2020', '2021'];
        const rows: DuckDBValue[][] = [['Cardiff', 100, 200]];
        setupMockDuckDB(columns, rows);

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Html });

        mockGetFilterTable.mockResolvedValue([]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');

        expect(output).toContain('<th>Area</th><th>2020</th><th>2021</th>');
        expect(output).toContain('<th>Cardiff</th><td>100</td><td>200</td>');
      });

      it('renders empty table when no columns', async () => {
        const mockResult = createMockDuckDBResult([], []);
        mockDuckdbRun.mockResolvedValue({ rowCount: 0 });
        mockDuckdbStream.mockResolvedValue(mockResult);

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Html });

        mockGetFilterTable.mockResolvedValue([]);
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        expect(output).toContain('<tbody>');
        expect(output).toContain('</tbody>');
      });
    });

    describe('Frontend output', () => {
      it('writes frontend JSON with page_info and data as arrays', async () => {
        const columns = ['Area', '2020', '2021'];
        const rows: DuckDBValue[][] = [
          ['Cardiff', 100, 200],
          ['Swansea', 150, 250]
        ];
        setupMockDuckDB(columns, rows);

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Frontend });

        mockGetFilterTable.mockResolvedValue([]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({ factTable: [], dimensions: [] });
        mockQueryRunnerQuery.mockResolvedValue([]);

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        const parsed = JSON.parse(output);

        expect(parsed.data).toHaveLength(2);
        // Frontend data is arrays, not objects
        expect(parsed.data[0]).toEqual(['Cardiff', 100, 200]);
        expect(parsed.data[1]).toEqual(['Swansea', 150, 250]);
        expect(parsed.page_info.total_records).toBe(50);
        expect(parsed.page_info.current_page).toBe(1);
      });
    });

    describe('Excel output', () => {
      it('writes a blank cell (not 0) when the DuckDB value is an empty string', async () => {
        // Number('') === 0, so without the explicit '' check the cell would be written as 0.
        const columns = ['Area', '2020', '2021'];
        const rows: DuckDBValue[][] = [['Cardiff', '' as unknown as DuckDBValue, 200]];
        setupMockDuckDB(columns, rows);

        const res = createMockBinaryResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Excel });

        mockGetFilterTable.mockResolvedValue([]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const buffer = await res.getBuffer();
        const workbook = new ExcelJS.Workbook();
        // @ts-expect-error ExcelJS types expect old Buffer, Node 24 returns Buffer<ArrayBuffer>
        await workbook.xlsx.load(buffer);

        const worksheet = workbook.getWorksheet(1)!;
        const dataRow = worksheet.getRow(2).values as (string | number | null)[];

        // ExcelJS row values are 1-indexed (index 0 is empty)
        expect(dataRow[1]).toBe('Cardiff');
        expect(dataRow[2]).toBeUndefined(); // blank cell — ExcelJS omits it from the values array
        expect(dataRow[3]).toBe(200);
      });
    });

    describe('column reordering aligns cell data with headers', () => {
      // DuckDB returns columns in its own order (e.g. ['Area', '2020', '2022', '2021']),
      // but getSortedPivotColumns reorders them (e.g. ['Area', '2022', '2021', '2020']).
      // These tests verify that cell values follow the reordered headers, not the original order.

      const duckDbColumns = ['Area', '2020', '2022', '2021'];
      const duckDbRows: DuckDBValue[][] = [
        ['Cardiff', 100, 300, 200],
        ['Swansea', 150, 350, 250]
      ];
      // Sorted order from lookup: 2022 DESC, 2021, 2020
      const sortedLookup = [{ description: '2022' }, { description: '2021' }, { description: '2020' }];

      function setupReorderingMocks() {
        setupMockDuckDB(duckDbColumns, duckDbRows);
        mockGetFilterTable.mockResolvedValue([
          { fact_table_column: 'period_col', dimension_name: 'Period', language: 'en' }
        ]);
        mockResolveDimensionToFactTableColumn.mockReturnValue('period_col');
        mockGetById.mockResolvedValue({
          factTable: [{ columnName: 'period_col' }],
          dimensions: [{ factTableColumn: 'period_col', type: DimensionType.DatePeriod }]
        });
        mockCubeQuery.mockResolvedValue(sortedLookup);
      }

      it('JSON: cell values match reordered column headers', async () => {
        setupReorderingMocks();
        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Json });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        const parsed = JSON.parse(output);

        expect(parsed.pivot[0]).toEqual({ Area: 'Cardiff', '2022': 300, '2021': 200, '2020': 100 });
        expect(parsed.pivot[1]).toEqual({ Area: 'Swansea', '2022': 350, '2021': 250, '2020': 150 });

        // Also verify key order in raw JSON string (skip the outer "pivot" key)
        const allKeys = [...output.matchAll(/"([^"]+)":/g)].map((m) => m[1]);
        const rowKeys = allKeys.filter((k) => k !== 'pivot');
        // Both rows should have keys in the same sorted order
        expect(rowKeys.slice(0, 4)).toEqual(['Area', '2022', '2021', '2020']);
        expect(rowKeys.slice(4, 8)).toEqual(['Area', '2022', '2021', '2020']);
      });

      it('CSV: cell values match reordered column headers', async () => {
        setupReorderingMocks();
        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Csv });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        const lines = output.trim().split('\n');

        expect(lines[0]).toBe('Area,2022,2021,2020');
        expect(lines[1]).toBe('Cardiff,300,200,100');
        expect(lines[2]).toBe('Swansea,350,250,150');
      });

      it('HTML: cell values match reordered column headers', async () => {
        setupReorderingMocks();
        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Html });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');

        expect(output).toContain('<th>Area</th><th>2022</th><th>2021</th><th>2020</th>');
        expect(output).toContain('<th>Cardiff</th><td>300</td><td>200</td><td>100</td>');
        expect(output).toContain('<th>Swansea</th><td>350</td><td>250</td><td>150</td>');
      });

      it('Frontend: cell values match reordered column headers', async () => {
        setupReorderingMocks();
        mockQueryRunnerQuery.mockResolvedValue([]);
        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Frontend });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const output = res.writtenData.join('');
        const parsed = JSON.parse(output);

        // Frontend data is arrays — values must be in sorted column order
        expect(parsed.data[0]).toEqual(['Cardiff', 300, 200, 100]);
        expect(parsed.data[1]).toEqual(['Swansea', 350, 250, 150]);
      });

      it('Excel: cell values match reordered column headers', async () => {
        setupReorderingMocks();
        const res = createMockBinaryResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Excel });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        const buffer = await res.getBuffer();
        const workbook = new ExcelJS.Workbook();
        // @ts-expect-error ExcelJS types expect old Buffer, Node 24 returns Buffer<ArrayBuffer>
        await workbook.xlsx.load(buffer);

        const worksheet = workbook.getWorksheet(1)!;
        const headerRow = worksheet.getRow(1).values as (string | number)[];
        const dataRow1 = worksheet.getRow(2).values as (string | number)[];
        const dataRow2 = worksheet.getRow(3).values as (string | number)[];

        // ExcelJS row values are 1-indexed (index 0 is empty)
        expect(headerRow.slice(1)).toEqual(['Area', '2022', '2021', '2020']);
        expect(dataRow1.slice(1)).toEqual(['Cardiff', 300, 200, 100]);
        expect(dataRow2.slice(1)).toEqual(['Swansea', 350, 250, 150]);
      });
    });

    describe('error handling', () => {
      it('releases DuckDB on success', async () => {
        setupMockDuckDB(['Area'], [['Cardiff']]);
        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Json });

        mockGetFilterTable.mockResolvedValue([]);
        mockGetById.mockResolvedValue({ dimensions: [] });

        await createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore);

        expect(mockReleaseDuckDB).toHaveBeenCalled();
      });

      it('releases DuckDB on error', async () => {
        mockDuckdbRun.mockResolvedValue(undefined);
        mockDuckdbStream.mockRejectedValue(new Error('Query failed'));

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Json });

        await expect(createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore)).rejects.toThrow();

        expect(mockReleaseDuckDB).toHaveBeenCalled();
      });

      it('throws BadRequestException for Binder Error', async () => {
        mockDuckdbRun.mockResolvedValue(undefined);
        mockDuckdbStream.mockRejectedValue(new Error('Binder Error: column not found'));

        const res = createMockStreamResponse();
        const queryStore = createMockQueryStore();
        const pageOptions = defaultPageOptions({ format: OutputFormats.Json });

        await expect(createPivotOutputUsingDuckDB(res, 'en', 'PIVOT (...)', pageOptions, queryStore)).rejects.toThrow(
          BadRequestException
        );
      });
    });
  });
});
