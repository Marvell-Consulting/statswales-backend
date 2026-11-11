/* eslint-disable @typescript-eslint/naming-convention */

import { PassThrough } from 'node:stream';
import { Response } from 'express';
import ExcelJS from 'exceljs';

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    trace: jest.fn()
  }
}));

jest.mock('i18next', () => ({
  t: jest.fn((key: string) => key)
}));

// preview-validator.ts (pulled in transitively by consumer-view.ts) imports the real i18next
// middleware, which calls .use()/.init() at module load time — stub it out entirely.
jest.mock('../../../src/middleware/translation', () => ({
  i18next: { t: jest.fn((key: string) => key) },
  SUPPORTED_LOCALES: ['en-GB', 'cy-GB'],
  AVAILABLE_LANGUAGES: ['en', 'cy'],
  t: jest.fn((key: string) => key)
}));

// Mock database manager: the pool client (for BEGIN/COMMIT/filter query/cursor) and the
// createQueryRunner()-based path used by the file's private getColumns().
const mockPoolQuery = jest.fn();
const mockPoolRelease = jest.fn();
const mockQueryRunnerQuery = jest.fn();
const mockQueryRunnerRelease = jest.fn();
jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: () => ({
      createQueryRunner: () => ({
        query: (...args: unknown[]) => mockQueryRunnerQuery(...args),
        release: (...args: unknown[]) => mockQueryRunnerRelease(...args)
      }),
      driver: {
        obtainMasterConnection: () =>
          Promise.resolve([{ query: (...args: unknown[]) => mockPoolQuery(...args), release: mockPoolRelease }])
      }
    })
  }
}));

// coreViewChooser hits the DB internally — stub it directly rather than mocking its collaborators.
jest.mock('../../../src/utils/consumer', () => ({
  coreViewChooser: jest.fn().mockResolvedValue('core_view_en'),
  dateColumnsFromDimensions: jest.fn().mockReturnValue(new Set()),
  sortFilterRows: jest.fn((rows) => rows),
  transformHierarchy: jest.fn()
}));

// Keep hold of each CSV stream the download creates, so tests can check what happens to it.
const mockCsvStreams: { destroyed: boolean }[] = [];
jest.mock('@fast-csv/format', () => {
  const actual = jest.requireActual('@fast-csv/format');
  return {
    ...actual,
    format: (...args: unknown[]) => {
      const stream = actual.format(...args);
      mockCsvStreams.push(stream);
      return stream;
    }
  };
});

// pg-cursor issues real protocol-level queries against a Client; stub it so cubeDBConn.query()
// can recognise cursor instances and return a controllable `.read()`.
const mockCursorRead = jest.fn();
const mockCursorClose = jest.fn();
jest.mock('pg-cursor', () => jest.fn().mockImplementation((query: string) => ({ __isCursor: true, query })));

import {
  createStreamingCSVFilteredView,
  createStreamingExcelFilteredView,
  createStreamingJSONFilteredView
} from '../../../src/services/consumer-view';
import { createStalledResponse, settle } from '../../helpers/stalled-response';
import { logger } from '../../../src/utils/logger';

function setupDbMocks(rows: Record<string, unknown>[]): void {
  mockQueryRunnerQuery.mockResolvedValue([]); // no column-order metadata -> defaults to SELECT *
  mockPoolQuery.mockImplementation((arg: unknown) => {
    if (arg === 'BEGIN' || arg === 'COMMIT' || arg === 'ROLLBACK') return Promise.resolve();
    if (arg && typeof arg === 'object' && (arg as { __isCursor?: boolean }).__isCursor) {
      let served = false;
      return {
        close: mockCursorClose.mockResolvedValue(undefined),
        read: mockCursorRead.mockImplementation(() => {
          if (served) return Promise.resolve([]);
          served = true;
          return Promise.resolve(rows);
        })
      };
    }
    // filter_table lookup for column/dimension names
    return Promise.resolve({ rows: [] });
  });
}

// Rows are padded so that buffers fill after a few hundred rows, whatever their exact sizes.
const PADDING = 'x'.repeat(100);
const pagedRow = (n: number): Record<string, unknown> => ({ id: n, label: `row ${n} ${PADDING}` });

// Serves `batches` batches of `batchSize` rows from the cursor, and counts how many batches have been read.
function setupPagedDbMocks(batches: number, batchSize: number): { reads: () => number } {
  let served = 0;
  mockQueryRunnerQuery.mockResolvedValue([]);
  mockPoolQuery.mockImplementation((arg: unknown) => {
    if (arg === 'BEGIN' || arg === 'COMMIT' || arg === 'ROLLBACK') return Promise.resolve();
    if (arg && typeof arg === 'object' && (arg as { __isCursor?: boolean }).__isCursor) {
      return {
        close: mockCursorClose.mockResolvedValue(undefined),
        read: mockCursorRead.mockImplementation(() => {
          if (served >= batches) return Promise.resolve([]);
          const start = served * batchSize;
          served++;
          return Promise.resolve(Array.from({ length: batchSize }, (_, i) => pagedRow(start + i)));
        })
      };
    }
    return Promise.resolve({ rows: [] });
  });
  return { reads: () => served };
}

function createMockStreamResponse(): Response & { writtenData: string[] } {
  const writtenData: string[] = [];
  const stream = new PassThrough();
  const originalWrite = stream.write.bind(stream);
  stream.write = ((chunk: any, encodingOrCallback?: any, callback?: any) => {
    if (chunk) writtenData.push(typeof chunk === 'string' ? chunk : chunk.toString());
    if (typeof encodingOrCallback === 'function') return originalWrite(chunk, encodingOrCallback);
    return originalWrite(chunk, encodingOrCallback, callback);
  }) as typeof stream.write;

  const res = stream as unknown as Response & { writtenData: string[] };
  res.writtenData = writtenData;
  res.setHeader = jest.fn().mockReturnThis();
  res.writeHead = jest.fn().mockReturnThis();
  res.flushHeaders = jest.fn();
  res.status = jest.fn().mockReturnThis();
  (res as any).headersSent = false;
  return res;
}

function createMockBinaryResponse(): Response & { getBuffer: () => Promise<Buffer> } {
  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));

  const res = stream as unknown as Response & { getBuffer: () => Promise<Buffer> };
  res.setHeader = jest.fn().mockReturnThis();
  res.writeHead = jest.fn().mockReturnThis();
  res.flushHeaders = jest.fn();
  res.status = jest.fn().mockReturnThis();
  (res as any).headersSent = false;
  res.getBuffer = () =>
    new Promise<Buffer>((resolve) => {
      stream.on('end', () => resolve(Buffer.concat(chunks)));
      if (stream.readableEnded) resolve(Buffer.concat(chunks));
    });
  return res;
}

describe('consumer-view (v1) service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockPoolRelease.mockResolvedValue(undefined);
    mockQueryRunnerRelease.mockResolvedValue(undefined);
  });

  describe('createStreamingCSVFilteredView', () => {
    it('neutralizes formula-injection payloads in headers and cell values (SW-1306 regression)', async () => {
      setupDbMocks([{ '=HYPERLINK("https://evil/")': '=1+1', Area: '+SUM(A1:A2)' }]);
      const res = createMockStreamResponse();

      await createStreamingCSVFilteredView(res, 'test-revision-id', 'en-GB');

      const output = res.writtenData.join('');
      expect(output).toContain(`'=HYPERLINK(""https://evil/"")`);
      expect(output).toContain(`'=1+1`);
      expect(output).toContain(`'+SUM(A1:A2)`);
    });
  });

  describe('when the client reads slowly', () => {
    const BATCHES = 100;
    const BATCH_SIZE = 100;
    const LAST = BATCHES * BATCH_SIZE - 1;

    it.each([
      ['CSV', createStreamingCSVFilteredView],
      ['JSON', createStreamingJSONFilteredView],
      ['Excel', createStreamingExcelFilteredView]
    ])('%s download waits for the client before reading more rows', async (_format, download) => {
      const db = setupPagedDbMocks(BATCHES, BATCH_SIZE);
      const res = createStalledResponse();

      const done = download(res, 'test-revision-id', 'en-GB');
      await settle(db.reads);

      expect(db.reads()).toBeLessThan(BATCHES / 2);

      res.resumeClient();
      await done;

      expect(db.reads()).toBe(BATCHES);
      expect(res.writableEnded).toBe(true);
      expect(mockPoolRelease).toHaveBeenCalledTimes(1);
    });

    it.each([
      ['CSV', createStreamingCSVFilteredView],
      ['JSON', createStreamingJSONFilteredView],
      ['Excel', createStreamingExcelFilteredView]
    ])('%s download treats a disconnect as normal, not an error', async (_format, download) => {
      const db = setupPagedDbMocks(BATCHES, BATCH_SIZE);
      const res = createStalledResponse();

      const done = download(res, 'test-revision-id', 'en-GB');
      await settle(db.reads);
      res.destroy();
      await done;

      expect(logger.error).not.toHaveBeenCalled();
    });

    it('CSV download destroys its CSV stream when the client disconnects, freeing what it has buffered', async () => {
      const db = setupPagedDbMocks(BATCHES, BATCH_SIZE);
      const res = createStalledResponse();

      const done = createStreamingCSVFilteredView(res, 'test-revision-id', 'en-GB');
      await settle(db.reads);
      res.destroy();
      await done;

      expect(mockCsvStreams[mockCsvStreams.length - 1].destroyed).toBe(true);
    });

    it('CSV download sends every row', async () => {
      setupPagedDbMocks(BATCHES, BATCH_SIZE);
      const res = createStalledResponse();
      res.resumeClient();

      await createStreamingCSVFilteredView(res, 'test-revision-id', 'en-GB');

      const lines = res.body().trim().split('\n');
      expect(lines).toHaveLength(BATCHES * BATCH_SIZE + 1);
      expect(lines[lines.length - 1]).toBe(`${LAST},${pagedRow(LAST).label}`);
    });

    it('JSON download sends every row as valid JSON', async () => {
      setupPagedDbMocks(BATCHES, BATCH_SIZE);
      const res = createStalledResponse();
      res.resumeClient();

      await createStreamingJSONFilteredView(res, 'test-revision-id', 'en-GB');

      expect(JSON.parse(res.body())).toHaveLength(BATCHES * BATCH_SIZE);
    });

    it.each([
      ['CSV', createStreamingCSVFilteredView],
      ['JSON', createStreamingJSONFilteredView],
      ['Excel', createStreamingExcelFilteredView]
    ])('%s download stops and releases the connection if the client disconnects', async (_format, download) => {
      const db = setupPagedDbMocks(BATCHES, BATCH_SIZE);
      const res = createStalledResponse();

      const done = download(res, 'test-revision-id', 'en-GB');
      await settle(db.reads);
      res.destroy();
      await done;

      expect(db.reads()).toBeLessThan(BATCHES / 2);
      // the connection is discarded rather than rolled back: closing the cursor and rolling back both wait for
      // Postgres on that connection, which can hang
      expect(mockPoolRelease).toHaveBeenCalledTimes(1);
      expect(mockPoolRelease).toHaveBeenCalledWith(true);
      expect(mockPoolQuery).not.toHaveBeenCalledWith('ROLLBACK');
    });
  });

  describe('createStreamingExcelFilteredView', () => {
    it('neutralizes formula-injection payloads in headers and cell values (SW-1306 regression)', async () => {
      setupDbMocks([{ '=HYPERLINK("https://evil/")': '-2+3+cmd|" /C calc"!A0', Area: 100 }]);
      const res = createMockBinaryResponse();

      await createStreamingExcelFilteredView(res, 'test-revision-id', 'en-GB');

      const buffer = await res.getBuffer();
      const workbook = new ExcelJS.Workbook();
      // @ts-expect-error ExcelJS types expect old Buffer, Node 24 returns Buffer<ArrayBuffer>
      await workbook.xlsx.load(buffer);

      const worksheet = workbook.getWorksheet(1)!;
      const headerRow = worksheet.getRow(1);
      const dataRow = worksheet.getRow(2);

      expect(headerRow.getCell(1).value).toBe(`'=HYPERLINK("https://evil/")`);
      expect(dataRow.getCell(1).value).toBe(`'-2+3+cmd|" /C calc"!A0`);
      expect(dataRow.getCell(2).value).toBe(100);
    });
  });
});
