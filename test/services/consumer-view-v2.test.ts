/* eslint-disable @typescript-eslint/naming-convention */

import { Readable, PassThrough } from 'node:stream';
import { Response } from 'express';

// Mock logger before other imports
jest.mock('../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    trace: jest.fn()
  }
}));

// Mock database manager
const mockCubeQuery = jest.fn();
const mockObtainMasterConnection = jest.fn();
const mockRelease = jest.fn();

jest.mock('../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: () => ({
      driver: {
        obtainMasterConnection: mockObtainMasterConnection
      },
      query: mockCubeQuery
    })
  }
}));

// Mock i18next
jest.mock('i18next', () => ({
  t: jest.fn((key: string) => key)
}));

// Mock DatasetRepository
const mockGetById = jest.fn();
jest.mock('../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: (...args: unknown[]) => mockGetById(...args)
  }
}));

// Mock consumer utils
jest.mock('../../src/utils/consumer', () => ({
  transformHierarchy: jest.fn((col, dim, data) => ({ column: col, dimension: dim, data }))
}));

// Mock column headers
jest.mock('../../src/utils/column-headers', () => ({
  getColumnHeaders: jest.fn().mockReturnValue([{ key: 'col1', label: 'Column 1' }])
}));

// Mock ConsumerDatasetDTO
jest.mock('../../src/dtos/consumer-dataset-dto', () => ({
  ConsumerDatasetDTO: {
    fromDataset: jest.fn().mockReturnValue({ id: 'dataset-id', title: 'Test Dataset' })
  }
}));

import { QueryStore } from '../../src/entities/query-store';
import { PageOptions } from '../../src/interfaces/page-options';
import { BadRequestException } from '../../src/exceptions/bad-request.exception';
import { Locale } from '../../src/enums/locale';
import { OutputFormats } from '../../src/enums/output-formats';
import {
  sendCsv,
  sendExcel,
  sendJson,
  sendHtml,
  sendFilters,
  sendFrontendView,
  buildDataQuery
} from '../../src/services/consumer-view-v2';

// Helper to create a mock QueryStore
function createMockQueryStore(overrides: Partial<QueryStore> = {}): QueryStore {
  return {
    id: 'test-query-store-id',
    hash: 'test-hash',
    datasetId: 'test-dataset-id',
    revisionId: 'test-revision-id',
    requestObject: { filters: [] },
    query: { 'en-GB': 'SELECT * FROM test_table', 'cy-GB': 'SELECT * FROM test_table' },
    totalLines: 100,
    columnMapping: [],
    createdAt: new Date(),
    updatedAt: new Date(),
    ...overrides
  } as QueryStore;
}

// Helper to create a mock Response that behaves like a writable stream
function createMockStreamResponse(): Response & { writtenData: string[]; writtenHeaders: Map<string, string> } {
  const writtenData: string[] = [];
  const writtenHeaders = new Map<string, string>();

  // Create a PassThrough stream as the base
  const stream = new PassThrough();

  // Track all written data
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

  // Add Response methods
  const res = stream as unknown as Response & { writtenData: string[]; writtenHeaders: Map<string, string> };
  res.writtenData = writtenData;
  res.writtenHeaders = writtenHeaders;
  res.setHeader = jest.fn((name: string, value: string | number | readonly string[]) => {
    writtenHeaders.set(name.toLowerCase(), String(value));
    return res;
  });
  res.flushHeaders = jest.fn();
  res.status = jest.fn().mockReturnThis();
  res.json = jest.fn();
  (res as any).headersSent = false;

  return res;
}

// Helper to create a mock readable stream that emits rows asynchronously
function createMockDbStream(rows: Record<string, unknown>[]): Readable {
  let index = 0;
  const stream = new Readable({
    objectMode: true,
    read() {
      // Use setImmediate to simulate async behavior
      setImmediate(() => {
        if (index < rows.length) {
          this.push(rows[index]);
          index++;
        } else {
          this.push(null);
        }
      });
    }
  });
  return stream;
}

// Helper to create a mock pool client
function createMockPoolClient(stream: Readable) {
  return {
    query: jest.fn().mockReturnValue(stream),
    release: mockRelease
  };
}

// Helper to create a mock stream that emits an error after some rows
function createErrorStream(rowsBeforeError: Record<string, unknown>[], error: Error): Readable {
  let index = 0;
  const stream = new Readable({
    objectMode: true,
    read() {
      setImmediate(() => {
        if (index < rowsBeforeError.length) {
          this.push(rowsBeforeError[index]);
          index++;
        } else {
          this.destroy(error);
        }
      });
    }
  });
  return stream;
}

describe('consumer-view-v2 service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockRelease.mockResolvedValue(undefined);
  });

  describe('sendCsv', () => {
    it('should stream CSV data to response', async () => {
      const rows = [
        { col1: 'value1', col2: 'value2' },
        { col1: 'value3', col2: 'value4' }
      ];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendCsv('SELECT * FROM test', queryStore, res);

      expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'text/csv');
      expect(res.setHeader).toHaveBeenCalledWith(
        'Content-Disposition',
        `attachment;filename=${queryStore.datasetId}.csv`
      );
      expect(mockRelease).toHaveBeenCalled();

      // Check that CSV data was written
      const output = res.writtenData.join('');
      expect(output).toContain('col1');
      expect(output).toContain('col2');
      expect(output).toContain('value1');
    });

    it('should write newline for empty result set', async () => {
      const mockStream = createMockDbStream([]);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendCsv('SELECT * FROM test', queryStore, res);

      expect(res.writtenData).toContain('\n');
    });

    it('should release connection on error', async () => {
      const error = new Error('Connection error');
      mockObtainMasterConnection.mockRejectedValue(error);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await expect(sendCsv('SELECT * FROM test', queryStore, res)).rejects.toThrow('Connection error');
    });

    it('should handle database query stream error', async () => {
      const dbError = new Error('Database query failed');
      const errorStream = createErrorStream([{ col1: 'value1' }], dbError);
      const mockPoolClient = createMockPoolClient(errorStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await expect(sendCsv('SELECT * FROM test', queryStore, res)).rejects.toThrow('Database query failed');
      expect(mockRelease).toHaveBeenCalled();
    });
  });

  describe('sendExcel', () => {
    it('should stream Excel data to response', async () => {
      const rows = [
        { col1: 'value1', col2: 100 },
        { col1: 'value2', col2: 200 }
      ];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendExcel('SELECT * FROM test', queryStore, res);

      expect(res.setHeader).toHaveBeenCalledWith(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      );
      expect(res.setHeader).toHaveBeenCalledWith(
        'Content-Disposition',
        `attachment;filename=${queryStore.datasetId}.xlsx`
      );
      expect(res.flushHeaders).toHaveBeenCalled();
      expect(mockRelease).toHaveBeenCalled();
    });

    it('should handle empty result set', async () => {
      const mockStream = createMockDbStream([]);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendExcel('SELECT * FROM test', queryStore, res);

      expect(res.flushHeaders).toHaveBeenCalled();
      expect(mockRelease).toHaveBeenCalled();
    });

    it('should release connection on error', async () => {
      const error = new Error('DB error');
      mockObtainMasterConnection.mockRejectedValue(error);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await expect(sendExcel('SELECT * FROM test', queryStore, res)).rejects.toThrow('DB error');
    });

    it('should handle database query stream error', async () => {
      const dbError = new Error('Excel query failed');
      const errorStream = createErrorStream([{ col1: 'value1' }], dbError);
      const mockPoolClient = createMockPoolClient(errorStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await expect(sendExcel('SELECT * FROM test', queryStore, res)).rejects.toThrow('Excel query failed');
      expect(mockRelease).toHaveBeenCalled();
    });
  });

  describe('sendJson', () => {
    it('should stream JSON array to response', async () => {
      const rows = [
        { col1: 'value1', col2: 'value2' },
        { col1: 'value3', col2: 'value4' }
      ];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendJson('SELECT * FROM test', queryStore, res);

      expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'application/json');
      expect(res.setHeader).toHaveBeenCalledWith(
        'Content-Disposition',
        `attachment;filename=${queryStore.datasetId}.json`
      );
      expect(mockRelease).toHaveBeenCalled();

      const output = res.writtenData.join('');
      expect(output).toContain('[');
      expect(output).toContain(']');
      expect(output).toContain('value1');
    });

    it('should output empty JSON array for no results', async () => {
      const mockStream = createMockDbStream([]);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendJson('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      expect(output).toBe('[]');
    });

    it('should add commas between JSON objects', async () => {
      const rows = [{ a: 1 }, { b: 2 }, { c: 3 }];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendJson('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      // Should be valid JSON array
      expect(() => JSON.parse(output)).not.toThrow();
      const parsed = JSON.parse(output);
      expect(parsed).toHaveLength(3);
    });

    it('should handle database query stream error', async () => {
      const dbError = new Error('JSON query failed');
      const errorStream = createErrorStream([{ col1: 'value1' }], dbError);
      const mockPoolClient = createMockPoolClient(errorStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await expect(sendJson('SELECT * FROM test', queryStore, res)).rejects.toThrow('JSON query failed');
      expect(mockRelease).toHaveBeenCalled();
    });
  });

  describe('sendHtml', () => {
    it('should stream HTML table to response', async () => {
      const rows = [
        { col1: 'value1', col2: 'value2' },
        { col1: 'value3', col2: 'value4' }
      ];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendHtml('SELECT * FROM test', queryStore, res);

      expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'text/html');
      expect(res.flushHeaders).toHaveBeenCalled();
      expect(mockRelease).toHaveBeenCalled();

      const output = res.writtenData.join('');
      expect(output).toContain('<!DOCTYPE html>');
      expect(output).toContain('<table>');
      expect(output).toContain('</table>');
      expect(output).toContain('<th>col1</th>');
      expect(output).toContain('<th>col2</th>');
      expect(output).toContain('<td>value1</td>');
    });

    it('should escape XSS in dataset ID', async () => {
      const mockStream = createMockDbStream([{ col: 'val' }]);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore({ datasetId: '<script>alert("xss")</script>' });
      const res = createMockStreamResponse();

      await sendHtml('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      expect(output).not.toContain('<script>alert');
      expect(output).toContain('&lt;script&gt;');
    });

    it('should escape XSS in column headers', async () => {
      const rows = [{ '<script>evil</script>': 'safe-value' }];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendHtml('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      expect(output).toContain('&lt;script&gt;evil&lt;/script&gt;');
      expect(output).not.toContain('<script>evil');
    });

    it('should escape XSS in cell values', async () => {
      const rows = [{ col1: '<img src=x onerror=alert(1)>' }];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendHtml('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      expect(output).not.toContain('<img');
      expect(output).toContain('&lt;img');
    });

    it('should handle null cell values', async () => {
      const rows = [{ col1: null, col2: 'value' }];
      const mockStream = createMockDbStream(rows);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendHtml('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      expect(output).toContain('<td></td>');
    });

    it('should handle empty result set', async () => {
      const mockStream = createMockDbStream([]);
      const mockPoolClient = createMockPoolClient(mockStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await sendHtml('SELECT * FROM test', queryStore, res);

      const output = res.writtenData.join('');
      expect(output).toContain('<tbody></tbody>');
    });

    it('should handle database query stream error', async () => {
      const dbError = new Error('HTML query failed');
      const errorStream = createErrorStream([{ col1: 'value1' }], dbError);
      const mockPoolClient = createMockPoolClient(errorStream);
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const queryStore = createMockQueryStore();
      const res = createMockStreamResponse();

      await expect(sendHtml('SELECT * FROM test', queryStore, res)).rejects.toThrow('HTML query failed');
      expect(mockRelease).toHaveBeenCalled();
    });
  });

  describe('sendFilters', () => {
    it('should return filter data grouped by column', async () => {
      const mockRows = [
        { fact_table_column: 'area', dimension_name: 'Area', sort_order: 1 },
        { fact_table_column: 'area', dimension_name: 'Area', sort_order: 2 },
        { fact_table_column: 'year', dimension_name: 'Year', sort_order: 1 }
      ];
      const mockPoolClient = {
        query: jest.fn().mockResolvedValue({ rows: mockRows }),
        release: mockRelease
      };
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const res = createMockStreamResponse();

      await sendFilters('SELECT * FROM filters', res);

      expect(res.json).toHaveBeenCalled();
      const jsonArg = (res.json as jest.Mock).mock.calls[0][0];
      expect(jsonArg).toHaveLength(2); // Two columns: area and year
      expect(mockRelease).toHaveBeenCalled();
    });

    it('should release connection on error', async () => {
      const mockPoolClient = {
        query: jest.fn().mockRejectedValue(new Error('Query failed')),
        release: mockRelease
      };
      mockObtainMasterConnection.mockResolvedValue([mockPoolClient]);

      const res = createMockStreamResponse();

      await expect(sendFilters('SELECT * FROM filters', res)).rejects.toThrow('Query failed');
      expect(mockRelease).toHaveBeenCalled();
    });
  });

  describe('sendFrontendView', () => {
    it('should return paginated data with headers', async () => {
      mockCubeQuery
        .mockResolvedValueOnce([{ fact_table_column: 'area', dimension_name: 'Area' }]) // filters query
        .mockResolvedValueOnce([{ code: 'N' }, { code: 'P' }]) // note_codes query
        .mockResolvedValueOnce([{ col1: 'val1', col2: 'val2' }]); // data query

      mockGetById.mockResolvedValue({ id: 'dataset-id', factTable: [], dimensions: [] });

      const queryStore = createMockQueryStore({ totalLines: 50 });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 1,
        pageSize: 10
      };
      const res = createMockStreamResponse();

      await sendFrontendView('SELECT * FROM data', queryStore, pageOptions, res);

      expect(res.json).toHaveBeenCalled();
      const jsonArg = (res.json as jest.Mock).mock.calls[0][0];
      expect(jsonArg).toHaveProperty('dataset');
      expect(jsonArg).toHaveProperty('data');
      expect(jsonArg).toHaveProperty('page_info');
      expect(jsonArg.page_info.current_page).toBe(1);
      expect(jsonArg.page_info.page_size).toBe(10);
      expect(jsonArg.page_info.total_records).toBe(50);
    });

    it('should use Welsh language for cy locale', async () => {
      mockCubeQuery
        .mockResolvedValueOnce([]) // filters query
        .mockResolvedValueOnce([]) // note_codes query
        .mockResolvedValueOnce([]); // data query

      mockGetById.mockResolvedValue({ id: 'dataset-id', factTable: [], dimensions: [] });

      const queryStore = createMockQueryStore();
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.WelshGb,
        pageNumber: 1,
        pageSize: 10
      };
      const res = createMockStreamResponse();

      await sendFrontendView('SELECT * FROM data', queryStore, pageOptions, res);

      // First query should use 'cy-gb' language
      expect(mockCubeQuery.mock.calls[0][0]).toContain('cy-gb');
    });

    it('should handle missing note codes gracefully', async () => {
      mockCubeQuery
        .mockResolvedValueOnce([]) // filters query
        .mockRejectedValueOnce(new Error('Note codes table missing')) // note_codes query throws
        .mockResolvedValueOnce([]); // data query

      mockGetById.mockResolvedValue({ id: 'dataset-id', factTable: [], dimensions: [] });

      const queryStore = createMockQueryStore();
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 1,
        pageSize: 10
      };
      const res = createMockStreamResponse();

      await sendFrontendView('SELECT * FROM data', queryStore, pageOptions, res);

      const jsonArg = (res.json as jest.Mock).mock.calls[0][0];
      expect(jsonArg.note_codes).toEqual([]);
    });
  });

  describe('buildDataQuery', () => {
    it('should build query with pagination', async () => {
      const queryStore = createMockQueryStore({
        query: { 'en-GB': 'SELECT * FROM test_table' },
        totalLines: 100
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 2,
        pageSize: 25
      };

      const result = await buildDataQuery(queryStore, pageOptions);

      expect(result).toContain('LIMIT');
      expect(result).toContain('OFFSET');
      expect(result).toContain('25'); // pageSize
    });

    it('should use Welsh query for Welsh locale', async () => {
      const queryStore = createMockQueryStore({
        query: {
          'en-GB': 'SELECT english FROM test_table',
          'cy-GB': 'SELECT welsh FROM test_table'
        },
        totalLines: 100
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.WelshGb,
        pageNumber: 1,
        pageSize: 10
      };

      const result = await buildDataQuery(queryStore, pageOptions);

      expect(result).toContain('welsh');
    });

    it('should fallback to English query if Welsh not available', async () => {
      const queryStore = createMockQueryStore({
        query: { 'en-GB': 'SELECT english FROM test_table' },
        totalLines: 100
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.WelshGb,
        pageNumber: 1,
        pageSize: 10
      };

      const result = await buildDataQuery(queryStore, pageOptions);

      expect(result).toContain('english');
    });

    it('should add ORDER BY for sort options', async () => {
      const queryStore = createMockQueryStore({
        query: { 'en-GB': 'SELECT * FROM test_table' },
        totalLines: 100
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: ['area|asc', 'year|desc'],
        locale: Locale.EnglishGb,
        pageNumber: 1,
        pageSize: 10
      };

      const result = await buildDataQuery(queryStore, pageOptions);

      expect(result).toContain('ORDER BY');
      expect(result).toContain('ASC');
      expect(result).toContain('DESC');
    });

    it('should throw BadRequestException for page number beyond total pages', async () => {
      const queryStore = createMockQueryStore({
        query: { 'en-GB': 'SELECT * FROM test_table' },
        totalLines: 50
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 10, // Way beyond total pages (50/25 = 2 pages)
        pageSize: 25
      };

      await expect(buildDataQuery(queryStore, pageOptions)).rejects.toThrow(BadRequestException);
    });

    it('should throw error when no query found for language', async () => {
      const queryStore = createMockQueryStore({
        query: {}, // No queries at all
        totalLines: 100
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 1,
        pageSize: 10
      };

      await expect(buildDataQuery(queryStore, pageOptions)).rejects.toThrow('No query found');
    });

    it('should return all rows when no pageSize provided', async () => {
      const queryStore = createMockQueryStore({
        query: { 'en-GB': 'SELECT * FROM test_table' },
        totalLines: 100
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 1
        // No pageSize
      };

      const result = await buildDataQuery(queryStore, pageOptions);

      expect(result).toContain('LIMIT');
      expect(result).toContain('100'); // Should use totalLines as limit
    });

    it('should handle zero total lines', async () => {
      const queryStore = createMockQueryStore({
        query: { 'en-GB': 'SELECT * FROM test_table' },
        totalLines: 0
      });
      const pageOptions: PageOptions = {
        format: OutputFormats.Frontend,
        sort: [],
        locale: Locale.EnglishGb,
        pageNumber: 1,
        pageSize: 10
      };

      // Should not throw - zero results is valid
      const result = await buildDataQuery(queryStore, pageOptions);
      expect(result).toBeDefined();
    });
  });
});
