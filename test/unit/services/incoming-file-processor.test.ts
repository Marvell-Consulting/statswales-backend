// === Mock setup (Jest hoists these above all imports) ===

const mockDuckDBRun = jest.fn();
const mockDuckDBRunAndReadAll = jest.fn();
const mockReleaseDuckDB = jest.fn();

jest.mock('../../../src/services/duckdb', () => ({
  DuckDBDatabases: {
    DataTables: 'data_tables',
    LookupTables: 'lookup_tables'
  },
  acquireDuckDB: jest.fn().mockImplementation(async () => ({
    duckdb: {
      run: (...args: unknown[]) => mockDuckDBRun(...args),
      runAndReadAll: (...args: unknown[]) => mockDuckDBRunAndReadAll(...args)
    },
    releaseDuckDB: mockReleaseDuckDB
  }))
}));

const mockQuery = jest.fn();
const mockRelease = jest.fn();
jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: jest.fn().mockReturnValue({
      createQueryRunner: jest.fn().mockReturnValue({
        query: (...args: unknown[]) => mockQuery(...args),
        release: (...args: unknown[]) => mockRelease(...args)
      })
    })
  }
}));

const mockFileServiceSaveStream = jest.fn();
jest.mock('../../../src/utils/get-file-service', () => ({
  getFileService: () => ({
    saveStream: (...args: unknown[]) => mockFileServiceSaveStream(...args)
  })
}));

// Replace fs.createReadStream with a tiny event emitter so the hash calc and
// the upload stream both complete predictably.
jest.mock('node:fs', () => {
  const original = jest.requireActual('node:fs');
  const { Readable } = jest.requireActual('node:stream');
  return {
    ...original,
    createReadStream: jest.fn().mockImplementation(() => Readable.from([Buffer.from('hello world')]))
  };
});

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    child: jest.fn().mockReturnValue({
      debug: jest.fn(),
      trace: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn()
    }),
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: jest.fn()
  }
}));

const mockDataTableFindOneByOrFail = jest.fn();
// Single factory providing both the constructor (so callers can do
// `new DataTable()`) and the static `findOneByOrFail` used by `getFilePreview`.
jest.mock('../../../src/entities/dataset/data-table', () => {
  const Ctor = function (this: Record<string, unknown>) {
    /* fields populated by caller */
  } as unknown as Record<string, unknown> & (new () => unknown);
  Ctor.findOneByOrFail = (...args: unknown[]) => mockDataTableFindOneByOrFail(...args);
  return { DataTable: Ctor };
});

jest.mock('../../../src/entities/dataset/data-table-description', () => ({
  DataTableDescription: jest.fn().mockImplementation(function (this: Record<string, unknown>) {
    Object.assign(this, {});
  })
}));

jest.mock('../../../src/validators/preview-validator', () => ({
  validateParams: jest.fn().mockReturnValue([])
}));

jest.mock('../../../src/utils/view-error-generators', () => ({
  viewErrorGenerators: jest.fn().mockImplementation((status: number) => ({ status, error: true })),
  viewGenerator: jest.fn().mockImplementation((_dataset, page, pageInfo, size, totalPages, headers, dataArray) => ({
    status: 200,
    page,
    pageInfo,
    size,
    totalPages,
    headers,
    dataArray
  }))
}));

// Import after mocks
import {
  getFilePreview,
  validateAndUpload,
  validateFileAndExtractTableInfo
} from '../../../src/services/incoming-file-processor';
import { DataTable } from '../../../src/entities/dataset/data-table';
import { FileType } from '../../../src/enums/file-type';
import { FileValidationErrorType, FileValidationException } from '../../../src/exceptions/validation-exception';
import { TempFile } from '../../../src/interfaces/temp-file';

// --- Helpers ---

function makeFile(overrides: Partial<TempFile> = {}): TempFile {
  return {
    path: '/tmp/test-file',
    originalname: 'test.csv',
    mimetype: 'text/csv',
    ...overrides
  };
}

function makeDataTable(fileType: FileType, id = 'dt-1'): DataTable {
  const dt = new DataTable() as DataTable & { fileType: FileType; id: string };
  dt.fileType = fileType;
  dt.id = id;
  return dt;
}

function makeReader(rows: Record<string, unknown>[]) {
  return {
    getRows: () => rows,
    getRowObjectsJson: () =>
      rows.map((row, index) => ({
        column_name: row.column_name ?? `col_${index}`,
        column_type: row.column_type ?? 'VARCHAR',
        index
      }))
  };
}

// --- Tests ---

describe('validateFileAndExtractTableInfo', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockDuckDBRun.mockReset();
    mockDuckDBRunAndReadAll.mockReset();
  });

  it('reads a CSV with UTF-8 encoding on the happy path', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'period' }, { column_name: 'value' }]));

    const result = await validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table');

    expect(result).toHaveLength(2);
    expect(result[0].columnName).toBe('period');
    // Verify that the first run was a UTF-8 attempt (utf-8 should be in the args)
    expect(mockDuckDBRun.mock.calls[0][0]).toContain('utf-8');
    expect(mockReleaseDuckDB).toHaveBeenCalled();
  });

  it('falls back to latin-1 encoding when the UTF-8 read fails', async () => {
    // First attempt (utf-8) fails, second (latin-1) succeeds, then create table + drop temp succeed
    mockDuckDBRun
      .mockRejectedValueOnce(new Error('utf-8 read failed'))
      .mockResolvedValueOnce(undefined) // latin-1 retry
      .mockResolvedValue(undefined); // subsequent runs
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'period' }, { column_name: 'value' }]));

    const dataTable = makeDataTable(FileType.Csv);
    const result = await validateFileAndExtractTableInfo(makeFile(), dataTable, 'data_table');

    expect(result).toHaveLength(2);
    expect(dataTable.encoding).toBe('latin-1');
    expect(mockDuckDBRun.mock.calls[1][0]).toContain('latin-1');
  });

  it('applies the utf-8 → latin-1 fallback to GzipCsv files as well as plain CSVs', async () => {
    mockDuckDBRun
      .mockRejectedValueOnce(new Error('utf-8 read failed'))
      .mockResolvedValueOnce(undefined) // latin-1 retry
      .mockResolvedValue(undefined); // subsequent runs
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'period' }, { column_name: 'value' }]));

    const dataTable = makeDataTable(FileType.GzipCsv);
    const result = await validateFileAndExtractTableInfo(
      makeFile({ originalname: 'data.csv.gz', mimetype: 'application/x-gzip' }),
      dataTable,
      'data_table'
    );

    expect(result).toHaveLength(2);
    expect(dataTable.encoding).toBe('latin-1');
    // First call used utf-8, second used latin-1 — confirms both attempts went
    // through the encoding branch rather than the no-encoding else branch.
    expect(mockDuckDBRun.mock.calls[0][0]).toContain('utf-8');
    expect(mockDuckDBRun.mock.calls[1][0]).toContain('latin-1');
  });

  it('reads a GzipCsv with UTF-8 on the happy path (no fallback needed)', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'period' }, { column_name: 'value' }]));

    const dataTable = makeDataTable(FileType.GzipCsv);
    await validateFileAndExtractTableInfo(
      makeFile({ originalname: 'data.csv.gz', mimetype: 'application/x-gzip' }),
      dataTable,
      'data_table'
    );

    expect(dataTable.encoding).toBe('utf-8');
    expect(mockDuckDBRun.mock.calls[0][0]).toContain('utf-8');
  });

  it('throws InvalidUnicode when latin-1 fallback also hits a unicode error', async () => {
    const err = new Error('Invalid unicode bytes in stream');
    // attach a stack property since the code checks `(error as DuckDBException).stack`
    err.stack = 'Error: Invalid unicode bytes';
    mockDuckDBRun.mockRejectedValueOnce(new Error('utf-8 read failed')).mockRejectedValueOnce(err);

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.InvalidUnicode
    });
  });

  it('throws InvalidCsv when DuckDB reports a CSV-parse error', async () => {
    const err = new Error('CSV Error on Line 3');
    err.stack = 'Error: CSV Error on Line 3';
    mockDuckDBRun.mockRejectedValueOnce(new Error('utf-8 failed')).mockRejectedValueOnce(err);

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.InvalidCsv
    });
  });

  it('throws unknown when DuckDB fails with an unrecognised error', async () => {
    const err = new Error('mystery');
    err.stack = 'Error: something we have not seen before';
    mockDuckDBRun.mockRejectedValueOnce(new Error('utf-8 failed')).mockRejectedValueOnce(err);

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.unknown
    });
  });

  it('reads an Excel file directly without the encoding-fallback path', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'period' }, { column_name: 'value' }]));

    const result = await validateFileAndExtractTableInfo(
      makeFile({ originalname: 'data.xlsx', mimetype: 'application/vnd.ms-excel' }),
      makeDataTable(FileType.Excel),
      'data_table'
    );

    expect(result).toHaveLength(2);
    // No latin-1 fallback should have run
    expect(mockDuckDBRun.mock.calls[0][0]).not.toContain('latin-1');
  });

  it('reads a JSON file directly without the encoding-fallback path', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'period' }]));

    const result = await validateFileAndExtractTableInfo(
      makeFile({ originalname: 'data.json', mimetype: 'application/json' }),
      makeDataTable(FileType.Json),
      'data_table'
    );

    expect(result).toHaveLength(1);
  });

  it('throws unknown when copying the data table to postgres fails', async () => {
    mockDuckDBRun
      .mockResolvedValueOnce(undefined) // read CSV
      .mockRejectedValueOnce(new Error('postgres unavailable'));

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.unknown
    });
  });

  it('throws unknown when extracting table headers fails', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockRejectedValueOnce(new Error('describe failed'));

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.unknown
    });
  });

  it('throws InvalidCsv when DuckDB returns no rows from DESCRIBE', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([]));

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.InvalidCsv
    });
  });

  it('throws InvalidCsv when a CSV produces only a single column from DESCRIBE', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'only-one' }]));

    await expect(
      validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv), 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.InvalidCsv
    });
  });

  it('routes lookup-table type writes to the lookup_tables schema with the _tmp suffix', async () => {
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValueOnce(makeReader([{ column_name: 'code' }, { column_name: 'label' }]));

    await validateFileAndExtractTableInfo(makeFile(), makeDataTable(FileType.Csv, 'lt-1'), 'lookup_table');

    // The copy-to-postgres statement (call index 1) should reference the lookup_tables schema and the _tmp suffix
    const copyCall = mockDuckDBRun.mock.calls[1][0] as string;
    expect(copyCall).toContain('lookup_tables');
    expect(copyCall).toContain('lt-1_tmp');
  });
});

describe('validateAndUpload — mimetype to file type mapping', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockDuckDBRun.mockReset();
    mockDuckDBRunAndReadAll.mockReset();
    mockFileServiceSaveStream.mockResolvedValue(undefined);

    // Default happy-path: every DuckDB call resolves, DESCRIBE returns two columns.
    mockDuckDBRun.mockResolvedValue(undefined);
    mockDuckDBRunAndReadAll.mockResolvedValue(makeReader([{ column_name: 'period' }, { column_name: 'value' }]));
  });

  const cases: { mimetype: string; expectedType: FileType; extension: string; originalname?: string }[] = [
    { mimetype: 'application/csv', expectedType: FileType.Csv, extension: 'csv' },
    { mimetype: 'text/csv', expectedType: FileType.Csv, extension: 'csv' },
    { mimetype: 'application/vnd.apache.parquet', expectedType: FileType.Parquet, extension: 'parquet' },
    { mimetype: 'application/parquet', expectedType: FileType.Parquet, extension: 'parquet' },
    { mimetype: 'application/json', expectedType: FileType.Json, extension: 'json' },
    { mimetype: 'application/vnd.ms-excel', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/msexcel', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/x-msexcel', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/x-ms-excel', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/x-excel', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/x-dos_ms_excel', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/xls', expectedType: FileType.Excel, extension: 'xlsx' },
    { mimetype: 'application/x-xls', expectedType: FileType.Excel, extension: 'xlsx' },
    {
      mimetype: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      expectedType: FileType.Excel,
      extension: 'xlsx'
    }
  ];

  it.each(cases)('maps $mimetype to $expectedType ($extension)', async ({ mimetype, expectedType, extension }) => {
    const result = await validateAndUpload(
      makeFile({ mimetype, originalname: `data.${extension}` }),
      'dataset-1',
      'data_table'
    );

    expect(result.fileType).toBe(expectedType);
    expect(result.mimeType).toBe(mimetype);
    expect(result.filename).toMatch(new RegExp(`\\.${extension.replace('.', '\\.')}$`));
  });

  it('detects GzipJson from the inner extension', async () => {
    const result = await validateAndUpload(
      makeFile({ mimetype: 'application/x-gzip', originalname: 'data.json.gz' }),
      'dataset-1',
      'data_table'
    );

    expect(result.fileType).toBe(FileType.GzipJson);
    expect(result.filename).toMatch(/\.json\.gz$/);
  });

  it('detects GzipCsv from the inner extension', async () => {
    const result = await validateAndUpload(
      makeFile({ mimetype: 'application/x-gzip', originalname: 'data.csv.gz' }),
      'dataset-1',
      'data_table'
    );

    expect(result.fileType).toBe(FileType.GzipCsv);
    expect(result.filename).toMatch(/\.csv\.gz$/);
  });

  it('throws UnknownFileFormat for a gzip with an unsupported inner extension', async () => {
    await expect(
      validateAndUpload(
        makeFile({ mimetype: 'application/x-gzip', originalname: 'data.parquet.gz' }),
        'dataset-1',
        'data_table'
      )
    ).rejects.toMatchObject({
      type: FileValidationErrorType.UnknownFileFormat
    });
  });

  it('throws UnknownMimeType for an unrecognised mimetype', async () => {
    await expect(
      validateAndUpload(makeFile({ mimetype: 'application/x-not-a-real-type' }), 'dataset-1', 'data_table')
    ).rejects.toMatchObject({
      type: FileValidationErrorType.UnknownMimeType
    });
  });

  it('throws DataLake (status 500) when the blob upload fails', async () => {
    mockFileServiceSaveStream.mockRejectedValueOnce(new Error('blob 503'));

    const promise = validateAndUpload(makeFile(), 'dataset-1', 'data_table');

    await expect(promise).rejects.toMatchObject({
      type: FileValidationErrorType.DataLake,
      status: 500
    });
  });

  it('computes a sha256 hash of the file contents', async () => {
    const result = await validateAndUpload(makeFile(), 'dataset-1', 'data_table');

    // sha256 of 'hello world' is the canonical b94d27b9...e9c2c0
    expect(result.hash).toBe('b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9');
  });
});

describe('FileValidationException', () => {
  it('defaults status to 400 and exposes a translation-friendly errorTag', () => {
    const err = new FileValidationException('boom', FileValidationErrorType.InvalidCsv);
    expect(err.status).toBe(400);
    expect(err.errorTag).toBe('errors.file_validation.invalid_csv');
    expect(err.type).toBe(FileValidationErrorType.InvalidCsv);
  });

  it('accepts a custom status code (e.g. 500 for DataLake failures)', () => {
    const err = new FileValidationException('blob', FileValidationErrorType.DataLake, 500);
    expect(err.status).toBe(500);
  });
});

describe('getFilePreview', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockQuery.mockReset();
    mockRelease.mockReset();
  });

  function makePreviewDataTable() {
    const dt = new DataTable() as DataTable & { id: string };
    dt.id = 'dt-1';
    return dt;
  }

  it('returns a paginated preview when both queries succeed', async () => {
    mockQuery.mockResolvedValueOnce([{ total_lines: 100 }]).mockResolvedValueOnce([
      { int_line_number: '1', region: 'Wales', value: 1 },
      { int_line_number: '2', region: 'Cymru', value: 2 }
    ]);

    const { DatasetRepository } = jest.requireMock('../../../src/repositories/dataset') as {
      DatasetRepository: { getById: jest.Mock };
    };
    DatasetRepository.getById.mockResolvedValueOnce({ id: 'ds-1', factTable: [] });
    mockDataTableFindOneByOrFail.mockResolvedValueOnce({ id: 'dt-1' });

    const result = await getFilePreview('ds-1', makePreviewDataTable(), 1, 50);

    expect(result).toMatchObject({ status: 200 });
    expect(mockRelease).toHaveBeenCalledTimes(2);
  });

  it('returns a 500 error view when the totals query fails', async () => {
    mockQuery.mockRejectedValueOnce(new Error('count failed'));

    const result = await getFilePreview('ds-1', makePreviewDataTable(), 1, 50);

    expect(result).toMatchObject({ status: 500 });
  });

  it('returns a 500 error view when the preview query fails', async () => {
    mockQuery.mockResolvedValueOnce([{ total_lines: 100 }]).mockRejectedValueOnce(new Error('select failed'));

    const result = await getFilePreview('ds-1', makePreviewDataTable(), 1, 50);

    expect(result).toMatchObject({ status: 500 });
  });

  it('returns 400 when the page parameters fail validation', async () => {
    const validator = jest.requireMock('../../../src/validators/preview-validator') as {
      validateParams: jest.Mock;
    };
    validator.validateParams.mockReturnValueOnce([{ field: 'page', message: 'out of range' }]);
    mockQuery.mockResolvedValueOnce([{ total_lines: 100 }]);

    const result = await getFilePreview('ds-1', makePreviewDataTable(), 99, 50);

    expect(result).toMatchObject({ status: 400 });
  });
});
