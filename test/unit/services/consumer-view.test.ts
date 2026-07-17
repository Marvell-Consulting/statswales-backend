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

// pg-cursor issues real protocol-level queries against a Client; stub it so cubeDBConn.query()
// can recognise cursor instances and return a controllable `.read()`.
const mockCursorRead = jest.fn();
jest.mock('pg-cursor', () => jest.fn().mockImplementation((query: string) => ({ __isCursor: true, query })));

import { createStreamingCSVFilteredView, createStreamingExcelFilteredView } from '../../../src/services/consumer-view';

function setupDbMocks(rows: Record<string, unknown>[]): void {
  mockQueryRunnerQuery.mockResolvedValue([]); // no column-order metadata -> defaults to SELECT *
  mockPoolQuery.mockImplementation((arg: unknown) => {
    if (arg === 'BEGIN' || arg === 'COMMIT' || arg === 'ROLLBACK') return Promise.resolve();
    if (arg && typeof arg === 'object' && (arg as { __isCursor?: boolean }).__isCursor) {
      let served = false;
      return {
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
