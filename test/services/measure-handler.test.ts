// === Mock setup (Jest hoists these above all imports) ===

const mockQuery = jest.fn();
const mockRelease = jest.fn();

jest.mock('../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: jest.fn().mockReturnValue({
      createQueryRunner: jest.fn().mockReturnValue({
        query: (...args: unknown[]) => mockQuery(...args),
        release: (...args: unknown[]) => mockRelease(...args)
      })
    })
  }
}));

jest.mock('../../src/services/duckdb', () => ({
  acquireDuckDB: jest.fn()
}));

jest.mock('../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    trace: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

// measure-handler imports `t` directly from 'i18next'
jest.mock('i18next', () => ({
  t: jest.fn((key: string) => {
    const parts = key.split('.');
    return parts[parts.length - 1] ?? key;
  })
}));

jest.mock('../../src/middleware/translation', () => ({
  SUPPORTED_LOCALES: ['en-GB', 'cy-GB'],
  AVAILABLE_LANGUAGES: ['en', 'cy'],
  t: jest.fn((key: string) => {
    const parts = key.split('.');
    return parts[parts.length - 1] ?? key;
  })
}));

jest.mock('../../src/services/cube-builder', () => ({
  FACT_TABLE_NAME: 'fact_table',
  VALIDATION_TABLE_NAME: 'validation_table',
  measureTableCreateStatement: jest.fn().mockReturnValue('CREATE TABLE measure_table (reference TEXT)'),
  postgresMeasureFormats: jest.fn().mockReturnValue(
    new Map([
      [
        'integer',
        {
          name: 'integer',
          method: 'WHEN measure.reference = |REF| THEN CAST(|COL| AS INTEGER)'
        }
      ]
    ])
  )
}));

jest.mock('../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: jest.fn()
  }
}));

jest.mock('../../src/entities/dataset/measure', () => ({
  Measure: {
    findOneByOrFail: jest.fn()
  }
}));

jest.mock('../../src/entities/dataset/measure-row', () => ({
  MeasureRow: Object.assign(
    jest.fn().mockImplementation(() => ({})),
    {
      delete: jest.fn().mockResolvedValue(undefined)
    }
  )
}));

jest.mock('../../src/entities/dataset/lookup-table', () => ({
  LookupTable: {
    delete: jest.fn().mockResolvedValue(undefined)
  }
}));

jest.mock('../../src/entities/dataset/measure-metadata', () => ({
  MeasureMetadata: jest.fn().mockImplementation(() => ({
    save: jest.fn().mockResolvedValue(undefined)
  }))
}));

jest.mock('../../src/utils/lookup-table-utils', () => ({
  convertDataTableToLookupTable: jest.fn(),
  lookForJoinColumn: jest.fn(),
  validateLookupTableReferenceValues: jest.fn().mockResolvedValue(null),
  validateLookupTableLanguages: jest.fn().mockResolvedValue(null),
  validateMeasureTableContent: jest.fn().mockResolvedValue(null),
  columnIdentification: jest.fn().mockImplementation((info: { columnName: string }) => ({
    name: info.columnName,
    lang: info.columnName.toLowerCase().includes('cy') ? 'cy-gb' : 'en-gb'
  })),
  languageMatcherCaseStatement: jest.fn().mockReturnValue("CASE WHEN LOWER(\"lang_col\") LIKE '%en%' THEN 'en-gb' END")
}));

jest.mock('../../src/utils/file-utils', () => ({
  loadFileIntoCube: jest.fn().mockResolvedValue(undefined)
}));

jest.mock('../../src/utils/get-file-service', () => ({
  getFileService: jest.fn().mockReturnValue({
    delete: jest.fn().mockResolvedValue(undefined)
  })
}));

jest.mock('../../src/utils/performance-reporting', () => ({
  performanceReporting: jest.fn()
}));

// === Imports (resolved after mocks) ===

import { Dataset } from '../../src/entities/dataset/dataset';
import { DataTable } from '../../src/entities/dataset/data-table';
import { DataTableDescription } from '../../src/entities/dataset/data-table-description';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { Measure } from '../../src/entities/dataset/measure';
import { Revision } from '../../src/entities/dataset/revision';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { FileType } from '../../src/enums/file-type';
import { acquireDuckDB } from '../../src/services/duckdb';
import { DatasetRepository } from '../../src/repositories/dataset';
import {
  lookForJoinColumn,
  convertDataTableToLookupTable,
  validateLookupTableReferenceValues,
  validateLookupTableLanguages,
  validateMeasureTableContent
} from '../../src/utils/lookup-table-utils';
import { getMeasurePreview, validateMeasureLookupTable } from '../../src/services/measure-handler';
import { ViewDTO, ViewErrDTO } from '../../src/dtos/view-dto';
import { MeasureLookupPatchDTO } from '../../src/dtos/measure-lookup-patch-dto';

// === Shared DuckDB mock helpers ===

const mockDuckdbRun = jest.fn().mockResolvedValue(undefined);
const mockDuckdbRunAndReadAll = jest.fn();
const mockReleaseDuckDB = jest.fn();

function setupDuckDBMock(rows: Record<string, unknown>[] = []) {
  mockDuckdbRunAndReadAll.mockResolvedValue({
    getRowObjectsJson: () => rows
  });
  (acquireDuckDB as jest.Mock).mockResolvedValue({
    duckdb: {
      run: mockDuckdbRun,
      runAndReadAll: mockDuckdbRunAndReadAll
    },
    releaseDuckDB: mockReleaseDuckDB
  });
}

const defaultMeasureRows = [
  {
    reference: 'REF1',
    language: 'en-gb',
    description: 'Description 1',
    format: 'integer',
    notes: null,
    sort_order: 1,
    decimals: 0,
    measure_type: null,
    hierarchy: null
  }
];

// === Mock lookup table (returned by convertDataTableToLookupTable) ===

const mockLookupTableSave = jest.fn().mockResolvedValue(undefined);
const mockLookupTableRemove = jest.fn().mockResolvedValue(undefined);
const mockLookupTable = {
  id: 'lt-1',
  isStatsWales2Format: false,
  save: mockLookupTableSave,
  remove: mockLookupTableRemove
};

// === Factory helpers ===

function makeFactTableColumn(columnName: string, columnType: FactTableColumnType): FactTableColumn {
  return { columnName, columnType, columnIndex: 0, columnDatatype: 'VARCHAR', id: 'dataset-1' } as FactTableColumn;
}

function makeDataTableDescription(columnName: string, idx = 0): DataTableDescription {
  return {
    id: 'dt-1',
    columnName,
    columnIndex: idx,
    columnDatatype: 'VARCHAR',
    factTableColumn: null
  } as unknown as DataTableDescription;
}

function makeProtoLookupTable(descriptionColumnNames: string[] = ['description_en', 'ref_code']): DataTable {
  return {
    id: 'dt-1',
    filename: 'measure.csv',
    originalFilename: 'measure.csv',
    mimeType: 'text/csv',
    fileType: FileType.Csv,
    encoding: 'utf-8',
    hash: 'abc123',
    dataTableDescriptions: descriptionColumnNames.map((name, i) => makeDataTableDescription(name, i)),
    action: 'add',
    sourceLocation: 'datalake'
  } as unknown as DataTable;
}

function makeMockMeasureEntity(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'measure-1',
    factTableColumn: 'measure_col',
    joinColumn: null,
    lookupTable: null,
    extractor: null,
    measureTable: null,
    dataset: { id: 'dataset-1' },
    save: jest.fn().mockResolvedValue({
      id: 'measure-1',
      factTableColumn: 'measure_col',
      joinColumn: 'ref_code',
      lookupTable: null,
      measureTable: [],
      metadata: []
    }),
    ...overrides
  };
}

function makeDataset(
  overrides: {
    noMeasureColumn?: boolean;
    noDataValues?: boolean;
    noDraftRevision?: boolean;
    measureTable?: unknown[] | null;
  } = {}
): Dataset {
  const factTableColumns: FactTableColumn[] = [];
  if (!overrides.noMeasureColumn) {
    factTableColumns.push(makeFactTableColumn('measure_col', FactTableColumnType.Measure));
  }
  if (!overrides.noDataValues) {
    factTableColumns.push(makeFactTableColumn('data_value', FactTableColumnType.DataValues));
  }

  const measureTable = overrides.measureTable !== undefined ? overrides.measureTable : null;

  return {
    id: 'dataset-1',
    createdAt: new Date('2024-01-01'),
    createdById: 'user-1',
    firstPublishedAt: null,
    archivedAt: null,
    replacementDatasetId: null,
    replacementAutoRedirect: false,
    dimensions: [],
    revisions: [],
    tasks: [],
    factTable: factTableColumns,
    measure: {
      id: 'measure-1',
      factTableColumn: 'measure_col',
      joinColumn: null,
      extractor: null,
      lookupTable: null,
      measureTable
    },
    draftRevision: overrides.noDraftRevision ? null : ({ id: 'revision-1', revisionIndex: 1 } as unknown as Revision)
  } as unknown as Dataset;
}

/** Minimal dataset shape satisfying DatasetDTO.fromDataset for viewGenerator calls */
function makeFullDatasetForRepository(): Dataset {
  return {
    id: 'dataset-1',
    createdAt: new Date('2024-01-01'),
    createdById: 'user-1',
    firstPublishedAt: null,
    archivedAt: null,
    replacementDatasetId: null,
    replacementAutoRedirect: false,
    dimensions: [],
    revisions: [],
    tasks: [],
    factTable: [],
    measure: null,
    draftRevision: null,
    startRevision: null,
    endRevision: null,
    publishedRevision: null,
    startDate: null,
    endDate: null
  } as unknown as Dataset;
}

// === Test setup ===

beforeEach(() => {
  jest.clearAllMocks();
  mockDuckdbRun.mockResolvedValue(undefined);
  (convertDataTableToLookupTable as jest.Mock).mockReturnValue(mockLookupTable);
  (lookForJoinColumn as jest.Mock).mockReturnValue('ref_code');
  (DatasetRepository.getById as jest.Mock).mockResolvedValue(makeFullDatasetForRepository());
  // Default: all validation utilities pass
  (validateLookupTableReferenceValues as jest.Mock).mockResolvedValue(null);
  (validateLookupTableLanguages as jest.Mock).mockResolvedValue(null);
  (validateMeasureTableContent as jest.Mock).mockResolvedValue(null);
});

// =============================================================================
// getMeasurePreview
// =============================================================================

describe('getMeasurePreview', () => {
  describe('routing to preview strategies', () => {
    it('uses the without-extractor path when measureTable is null', async () => {
      const dataset = makeDataset({ measureTable: null });

      mockQuery.mockResolvedValueOnce([{ measure_col: 'REF1' }]); // preview query

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewDTO;

      expect(result).toMatchObject({ current_page: 1 });
      expect(DatasetRepository.getById).toHaveBeenCalledWith('dataset-1');
    });

    it('uses the without-extractor path when measureTable is an empty array', async () => {
      const dataset = makeDataset({ measureTable: [] });

      mockQuery.mockResolvedValueOnce([{ measure_col: 'REF1' }]);

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewDTO;

      expect(result).toMatchObject({ current_page: 1 });
    });

    it('uses the without-extractor path when revisionTasks.measure is truthy, even if measureTable has rows', async () => {
      const dataset = makeDataset({
        measureTable: [
          { id: 'measure-1', reference: 'REF1', language: 'en-gb', description: 'Desc', format: 'integer' }
        ]
      });

      mockQuery.mockResolvedValueOnce([{ measure_col: 'REF1' }]);

      // revisionTasks.measure causes the extractor path to be bypassed
      const result = (await getMeasurePreview(dataset, 'en-GB', {
        dimensions: [],
        measure: { id: 'measure-1', lookupTableUpdated: true }
      })) as ViewDTO;

      expect(result).toMatchObject({ current_page: 1 });
    });

    it('uses the with-extractor path when measureTable has rows', async () => {
      const dataset = makeDataset({
        measureTable: [
          { id: 'measure-1', reference: 'REF1', language: 'en-gb', description: 'Desc', format: 'integer' }
        ]
      });

      mockQuery.mockResolvedValueOnce([
        {
          reference: 'REF1',
          description: 'Desc 1',
          notes: null,
          sort_order: 1,
          format: 'integer',
          decimals: 0,
          measure_type: null,
          hierarchy: null
        }
      ]);

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewDTO;

      expect(result).toMatchObject({ current_page: 1, page_size: 1 });
    });
  });

  describe('getMeasurePreviewWithoutExtractor', () => {
    it('returns a ViewDTO with the correct header derived from the measure column', async () => {
      const dataset = makeDataset({ measureTable: null });
      mockQuery.mockResolvedValueOnce([{ measure_col: 'REF1' }, { measure_col: 'REF2' }]);

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewDTO;

      expect(result.headers).toHaveLength(1);
      expect(result.headers[0].name).toBe('measure_col');
      expect(result.data).toEqual([['REF1'], ['REF2']]);
    });

    it('returns an error view when the preview query fails', async () => {
      const dataset = makeDataset({ measureTable: null });
      mockQuery.mockRejectedValueOnce(new Error('db error'));

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.measure.unknown_error');
    });
  });

  describe('getMeasurePreviewWithExtractor', () => {
    it('returns an empty ViewDTO when the measure table has no rows for the requested language', async () => {
      const dataset = makeDataset({
        measureTable: [
          { id: 'measure-1', reference: 'REF1', language: 'en-gb', description: 'Desc', format: 'integer' }
        ]
      });

      mockQuery.mockResolvedValueOnce([]); // empty result

      const result = (await getMeasurePreview(dataset, 'cy-GB')) as ViewDTO;

      expect(result.data).toEqual([]);
      expect(result.headers).toEqual([]);
    });

    it('returns a ViewDTO with measure table headers and data', async () => {
      const dataset = makeDataset({
        measureTable: [
          { id: 'measure-1', reference: 'REF1', language: 'en-gb', description: 'Desc', format: 'integer' }
        ]
      });

      mockQuery.mockResolvedValueOnce([
        {
          reference: 'REF1',
          description: 'Desc 1',
          notes: null,
          sort_order: 1,
          format: 'integer',
          decimals: 0,
          measure_type: null,
          hierarchy: null
        },
        {
          reference: 'REF2',
          description: 'Desc 2',
          notes: null,
          sort_order: 2,
          format: 'integer',
          decimals: 0,
          measure_type: null,
          hierarchy: null
        }
      ]);

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewDTO;

      expect(result.headers).toHaveLength(8);
      expect(result.headers[0].name).toBe('reference');
      expect(result.data).toHaveLength(2);
      // page_size is capped at sampleSize (5)
      expect(result.page_size).toBe(2);
    });

    it('returns an error view when the extractor preview query fails', async () => {
      const dataset = makeDataset({
        measureTable: [
          { id: 'measure-1', reference: 'REF1', language: 'en-gb', description: 'Desc', format: 'integer' }
        ]
      });

      mockQuery.mockRejectedValueOnce(new Error('query failed'));

      const result = (await getMeasurePreview(dataset, 'en-GB')) as ViewErrDTO;

      expect(result.status).toBe(500);
    });
  });
});

// =============================================================================
// validateMeasureLookupTable
// =============================================================================

describe('validateMeasureLookupTable', () => {
  // tableMatcher that forces isSW2Format = false (language_column provided)
  const tableMatcher: MeasureLookupPatchDTO = {
    description_columns: ['description_en'],
    language_column: 'lang_col'
  };

  // Proto lookup table with enough columns for auto-detection to pass
  const validProtoTable = makeProtoLookupTable(['description_en', 'ref_code', 'lang_col']);

  function setupHappyPathMocks(measureRowOverrides?: Record<string, unknown>[]) {
    const rows = measureRowOverrides ?? defaultMeasureRows;
    setupDuckDBMock(rows);

    // Postgres: create measure table batch, format validation per row, preview SELECT, DROP TABLE
    mockQuery.mockResolvedValueOnce(undefined); // create measure table
    for (let i = 0; i < rows.length; i++) {
      mockQuery.mockResolvedValueOnce(undefined); // format validation
    }
    mockQuery.mockResolvedValueOnce([
      {
        reference: 'REF1',
        description: 'Desc 1',
        notes: null,
        sort_order: 1,
        format: 'integer',
        decimals: 0,
        measure_type: null,
        hierarchy: null
      }
    ]); // preview SELECT
    mockQuery.mockResolvedValueOnce(undefined); // DROP TABLE in finally

    // updateMeasure: Measure.findOneByOrFail (1st call)
    (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());
    // cleanUpMeasure: Measure.findOneByOrFail (2nd call)
    (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());
  }

  describe('early-exit precondition checks', () => {
    it('returns 500 error when no measure column exists in factTable', async () => {
      const dataset = makeDataset({ noMeasureColumn: true });

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dataset.measure_not_found');
    });

    it('returns 500 error when dataset has no draft revision', async () => {
      const dataset = makeDataset({ noDraftRevision: true });

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dataset.draft_not_found');
    });

    it('returns 400 error when no description columns are detected in the lookup table', async () => {
      // Table with no columns containing 'description'
      const tableWithoutDescription = makeProtoLookupTable(['ref_code', 'lang_col', 'sort_order']);
      const dataset = makeDataset();

      const result = (await validateMeasureLookupTable(
        tableWithoutDescription,
        dataset,
        '/tmp/file.csv',
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.no_description_columns');
    });

    it('returns 400 error when lookForJoinColumn throws', async () => {
      const dataset = makeDataset();
      (lookForJoinColumn as jest.Mock).mockImplementationOnce(() => {
        throw new Error('No join column found');
      });

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.no_join_column');
    });

    it('returns 400 error when lookForJoinColumn returns undefined', async () => {
      const dataset = makeDataset();
      (lookForJoinColumn as jest.Mock).mockReturnValueOnce(undefined);

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.no_join_column');
    });

    it('returns 400 error when createExtractor throws due to columnIdentification failing', async () => {
      const dataset = makeDataset();
      const { columnIdentification } = jest.requireMock('../../src/utils/lookup-table-utils');
      columnIdentification.mockImplementationOnce(() => {
        throw new Error('column identification error');
      });

      // No tableMatcher → auto-detection path; columnIdentification is called inside map()
      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.no_description_columns');
    });
  });

  describe('DuckDB / createMeasureTable errors', () => {
    it('returns 400 error when DuckDB insertion fails', async () => {
      setupDuckDBMock();
      const dataset = makeDataset();
      // DuckDB run fails on the INSERT step (3rd call: create table, loadFile is mocked separately, INSERT fails)
      mockDuckdbRun
        .mockResolvedValueOnce(undefined) // measureTableCreateStatement
        .mockRejectedValueOnce(new Error('insertion failed')); // INSERT batch

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
    });

    it('returns 400 error when DuckDB fails with a Conversion/decimal error', async () => {
      setupDuckDBMock();
      const dataset = makeDataset();
      mockDuckdbRun
        .mockResolvedValueOnce(undefined) // measureTableCreateStatement
        .mockRejectedValueOnce(Object.assign(new Error('cannot convert decimal value'), { errorType: 'Conversion' })); // INSERT batch

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
    });

    it('returns 400 error when the DuckDB copy to lookup_tables_db fails', async () => {
      setupDuckDBMock();
      const dataset = makeDataset();
      mockDuckdbRun
        .mockResolvedValueOnce(undefined) // measureTableCreateStatement
        .mockResolvedValueOnce(undefined) // INSERT batch
        .mockRejectedValueOnce(new Error('copy failed')); // DROP TABLE lookup_tables_db

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
    });
  });

  describe('postgres errors after DuckDB success', () => {
    it('returns 500 error when postgres create-measure-table query fails', async () => {
      setupDuckDBMock(defaultMeasureRows);
      const dataset = makeDataset();

      mockQuery.mockRejectedValueOnce(new Error('postgres error on create'));

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dimension_validation.lookup_table_loading_failed');
      expect(mockLookupTableRemove).toHaveBeenCalled();
    });
  });

  describe('validation failures', () => {
    it('returns error and removes lookup table when validateLookupTableReferenceValues finds mismatches', async () => {
      setupDuckDBMock(defaultMeasureRows);
      const dataset = makeDataset();

      mockQuery.mockResolvedValueOnce(undefined); // create measure table
      (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());

      const referenceError: ViewErrDTO = {
        status: 400,
        dataset_id: 'dataset-1',
        errors: [
          { field: 'reference', message: { key: 'errors.lookup.missing_values', params: {} }, user_message: [] }
        ],
        extension: {}
      };
      (validateLookupTableReferenceValues as jest.Mock).mockResolvedValueOnce(referenceError);

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result).toBe(referenceError);
      expect(mockLookupTableRemove).toHaveBeenCalled();
    });

    it('returns 500 error when no DataValues column is present in the fact table', async () => {
      setupDuckDBMock(defaultMeasureRows);
      const dataset = makeDataset({ noDataValues: true });

      mockQuery.mockResolvedValueOnce(undefined); // create measure table
      (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.unknown_error');
    });

    it('returns 400 error when format validation query fails for a measure row', async () => {
      setupDuckDBMock(defaultMeasureRows);
      const dataset = makeDataset();

      mockQuery.mockResolvedValueOnce(undefined); // create measure table
      (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());
      mockQuery.mockRejectedValueOnce(new Error('wrong type cast')); // format validation fails

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.format_error');
    });

    it('returns error and removes lookup table when validateLookupTableLanguages finds issues', async () => {
      setupDuckDBMock(defaultMeasureRows);
      const dataset = makeDataset();

      mockQuery.mockResolvedValueOnce(undefined); // create measure table
      mockQuery.mockResolvedValueOnce(undefined); // format validation
      (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());

      const languageError: ViewErrDTO = {
        status: 400,
        dataset_id: 'dataset-1',
        errors: [
          { field: 'language', message: { key: 'errors.lookup.language_mismatch', params: {} }, user_message: [] }
        ],
        extension: {}
      };
      (validateLookupTableLanguages as jest.Mock).mockResolvedValueOnce(languageError);

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result).toBe(languageError);
      expect(mockLookupTableRemove).toHaveBeenCalled();
    });

    it('returns error and removes lookup table when validateMeasureTableContent finds issues', async () => {
      setupDuckDBMock(defaultMeasureRows);
      const dataset = makeDataset();

      mockQuery.mockResolvedValueOnce(undefined); // create measure table
      mockQuery.mockResolvedValueOnce(undefined); // format validation
      (Measure.findOneByOrFail as jest.Mock).mockResolvedValueOnce(makeMockMeasureEntity());

      const contentError: ViewErrDTO = {
        status: 400,
        dataset_id: 'dataset-1',
        errors: [{ field: 'content', message: { key: 'errors.lookup.invalid_content', params: {} }, user_message: [] }],
        extension: {}
      };
      (validateMeasureTableContent as jest.Mock).mockResolvedValueOnce(contentError);

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result).toBe(contentError);
      expect(mockLookupTableRemove).toHaveBeenCalled();
    });
  });

  describe('preview generation errors', () => {
    // These tests set up mocks directly to avoid queue interference with setupHappyPathMocks

    function setupPreviewTestMocks() {
      setupDuckDBMock(defaultMeasureRows);
      (Measure.findOneByOrFail as jest.Mock)
        .mockResolvedValueOnce(makeMockMeasureEntity()) // updateMeasure
        .mockResolvedValueOnce(makeMockMeasureEntity()); // cleanUpMeasure
      mockQuery.mockResolvedValueOnce(undefined); // create measure table
      mockQuery.mockResolvedValueOnce(undefined); // format validation per row
    }

    it('returns 500 error when the preview query fails', async () => {
      setupPreviewTestMocks();
      const dataset = makeDataset();

      mockQuery.mockRejectedValueOnce(new Error('preview query failed')); // preview SELECT throws
      mockQuery.mockResolvedValueOnce(undefined); // DROP TABLE in finally still runs

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.measure.unknown_error');
    });

    it('returns 404 error when the preview result is empty', async () => {
      setupPreviewTestMocks();
      const dataset = makeDataset();

      mockQuery.mockResolvedValueOnce([]); // preview SELECT returns empty
      mockQuery.mockResolvedValueOnce(undefined); // DROP TABLE in finally

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewErrDTO;

      expect(result.status).toBe(404);
      expect(result.errors[0].message.key).toBe('errors.measure.empty_table');
    });
  });

  describe('happy path', () => {
    it('returns a ViewDTO on full success', async () => {
      setupHappyPathMocks();
      const dataset = makeDataset();

      const result = (await validateMeasureLookupTable(
        validProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        tableMatcher
      )) as ViewDTO;

      expect(result).toMatchObject({ current_page: 1 });
      expect(result.data).toHaveLength(1);
      expect(result.headers).toBeDefined();
    });

    it('saves the lookup table, measure, and measure metadata during the happy path', async () => {
      setupHappyPathMocks();
      const dataset = makeDataset();

      await validateMeasureLookupTable(validProtoTable, dataset, '/tmp/file.csv', 'en-GB', tableMatcher);

      expect(mockLookupTableSave).toHaveBeenCalled();
      expect(Measure.findOneByOrFail).toHaveBeenCalledTimes(2); // updateMeasure + cleanUpMeasure
    });

    it('releases all query runners even on success', async () => {
      setupHappyPathMocks();
      const dataset = makeDataset();

      await validateMeasureLookupTable(validProtoTable, dataset, '/tmp/file.csv', 'en-GB', tableMatcher);

      // createMeasureTableRunner (void release), validateDataValuesRunner (awaited), createPreviewRunner (void release)
      expect(mockRelease).toHaveBeenCalled();
    });

    it('handles isSW2Format=true when two locale description columns are provided', async () => {
      // SW2 format: two description columns (one per locale), no language_column
      const sw2tableMatcher: MeasureLookupPatchDTO = {
        description_columns: ['description_en', 'description_cy']
        // no language_column → isSW2Format = true
      };
      const sw2ProtoTable = makeProtoLookupTable(['description_en', 'description_cy', 'ref_code']);

      setupHappyPathMocks();
      const dataset = makeDataset();

      const result = (await validateMeasureLookupTable(
        sw2ProtoTable,
        dataset,
        '/tmp/file.csv',
        'en-GB',
        sw2tableMatcher
      )) as ViewDTO;

      expect(result).toMatchObject({ current_page: 1 });
    });
  });
});
