// === Mock setup (Jest hoists these above all imports) ===

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

jest.mock('node:crypto', () => ({
  randomUUID: jest.fn().mockReturnValue('mock-cube-id')
}));

jest.mock('i18next', () => ({
  t: jest.fn((key: string) => {
    const parts = key.split('.');
    return parts[parts.length - 1] ?? key;
  })
}));

jest.mock('../../../src/middleware/translation', () => ({
  SUPPORTED_LOCALES: ['en-GB', 'cy-GB'],
  AVAILABLE_LANGUAGES: ['en', 'cy']
}));

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    trace: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

jest.mock('../../../src/services/cube-builder', () => ({
  FACT_TABLE_NAME: 'fact_table'
}));

jest.mock('../../../src/utils/lookup-table-utils', () => ({
  convertDataTableToLookupTable: jest.fn(),
  lookForPossibleJoinColumn: jest.fn(),
  validateLookupTableLanguages: jest.fn().mockResolvedValue(null),
  validateLookupTableHierarchyValues: jest.fn().mockResolvedValue(undefined),
  columnIdentification: jest.fn().mockImplementation((info: { columnName: string }) => ({
    name: info.columnName,
    lang: info.columnName.toLowerCase().includes('cy') ? 'cy-gb' : 'en-gb'
  }))
}));

jest.mock('../../../src/services/dimension-processor', () => ({
  cleanUpDimension: jest.fn().mockResolvedValue(undefined)
}));

jest.mock('../../../src/utils/mock-cube-handler', () => ({
  createPostgresValidationSchema: jest.fn().mockResolvedValue(undefined),
  cleanUpPostgresValidationSchema: jest.fn().mockResolvedValue(undefined),
  saveValidatedLookupTableToDatabase: jest.fn().mockResolvedValue(undefined)
}));

jest.mock('../../../src/utils/file-utils', () => ({
  convertLookupTableToSW3Format: jest.fn().mockResolvedValue(undefined)
}));

jest.mock('../../../src/utils/preview-generator', () => ({
  previewGenerator: jest.fn()
}));

jest.mock('../../../src/entities/dataset/dimension', () => ({
  Dimension: {
    findOneByOrFail: jest.fn()
  }
}));

// === Imports (resolved after mocks) ===

import { Dataset } from '../../../src/entities/dataset/dataset';
import { DataTable } from '../../../src/entities/dataset/data-table';
import { DataTableDescription } from '../../../src/entities/dataset/data-table-description';
import { FactTableColumn } from '../../../src/entities/dataset/fact-table-column';
import { Dimension } from '../../../src/entities/dataset/dimension';
import { Revision } from '../../../src/entities/dataset/revision';
import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { DimensionType } from '../../../src/enums/dimension-type';
import { FileType } from '../../../src/enums/file-type';
import { ViewDTO, ViewErrDTO } from '../../../src/dtos/view-dto';
import {
  convertDataTableToLookupTable,
  lookForPossibleJoinColumn,
  validateLookupTableLanguages
} from '../../../src/utils/lookup-table-utils';
import {
  createPostgresValidationSchema,
  cleanUpPostgresValidationSchema,
  saveValidatedLookupTableToDatabase
} from '../../../src/utils/mock-cube-handler';
import { convertLookupTableToSW3Format } from '../../../src/utils/file-utils';
import { previewGenerator } from '../../../src/utils/preview-generator';
import {
  confirmJoinColumnAndValidateReferenceValues,
  validateLookupTable
} from '../../../src/services/lookup-table-handler';

// === Shared mock objects ===

const mockLookupTableSave = jest.fn();
const mockLookupTable = {
  id: 'lt-1',
  isStatsWales2Format: false,
  save: mockLookupTableSave
};

const mockDimensionSave = jest.fn();

// === Factory helpers ===

function makeDataTableDescription(columnName: string, idx = 0): DataTableDescription {
  return {
    id: 'dtd-1',
    columnName,
    columnIndex: idx,
    columnDatatype: 'VARCHAR',
    factTableColumn: null
  } as unknown as DataTableDescription;
}

function makeProtoLookupTable(columnNames: string[] = ['description_en', 'ref_code']): DataTable {
  return {
    id: 'dt-1',
    filename: 'lookup.csv',
    originalFilename: 'lookup.csv',
    mimeType: 'text/csv',
    fileType: FileType.Csv,
    encoding: 'utf-8',
    hash: 'abc123',
    dataTableDescriptions: columnNames.map((name, i) => makeDataTableDescription(name, i)),
    action: 'add',
    sourceLocation: 'datalake'
  } as unknown as DataTable;
}

function makeFactTableColumn(columnName: string, columnType: FactTableColumnType): FactTableColumn {
  return {
    columnName,
    columnType,
    columnIndex: 0,
    columnDatatype: 'VARCHAR',
    id: 'dataset-1'
  } as FactTableColumn;
}

function makeDimension(overrides: Partial<{ lookupTable: unknown }> = {}): Dimension {
  return {
    id: 'dim-1',
    factTableColumn: 'dim_col',
    joinColumn: null,
    lookupTable: null,
    type: DimensionType.RawDimension,
    extractor: null,
    ...overrides
  } as unknown as Dimension;
}

function makeMockDimensionEntity(): Record<string, unknown> {
  return {
    id: 'dim-1',
    factTableColumn: 'dim_col',
    joinColumn: null,
    lookupTable: null,
    type: DimensionType.RawDimension,
    extractor: null,
    save: mockDimensionSave
  };
}

function makeDataset(overrides: { noDimensionColumn?: boolean } = {}): Dataset {
  const factTable: FactTableColumn[] = [];
  if (!overrides.noDimensionColumn) {
    factTable.push(makeFactTableColumn('dim_col', FactTableColumnType.Dimension));
  }
  factTable.push(makeFactTableColumn('data_value', FactTableColumnType.DataValues));

  return {
    id: 'dataset-1',
    factTable,
    dimensions: [],
    draftRevision: { id: 'revision-1', revisionIndex: 1 } as unknown as Revision
  } as unknown as Dataset;
}

const mockViewDTO: ViewDTO = {
  current_page: 1,
  page_size: 5,
  total_pages: 1,
  headers: [{ index: 0, name: 'ref', source_type: FactTableColumnType.Unknown }],
  data: [['val1']],
  dataset: {} as never
};

// Sets up mockQuery for confirmJoinColumnAndValidateReferenceValues happy path:
// call 1 → count query, call 2 → LEFT JOIN with 0 missing rows (join column found)
function setupJoinColumnFoundMocks(total = 5) {
  mockQuery.mockResolvedValueOnce([{ total }]).mockResolvedValueOnce([]);
}

// === Test setup ===

const draftRevision = { id: 'revision-1', revisionIndex: 1 } as unknown as Revision;

beforeEach(() => {
  jest.clearAllMocks();
  mockLookupTableSave.mockResolvedValue(undefined);
  mockDimensionSave.mockResolvedValue(undefined);
  (convertDataTableToLookupTable as jest.Mock).mockReturnValue(mockLookupTable);
  (lookForPossibleJoinColumn as jest.Mock).mockReturnValue(['ref_code']);
  (validateLookupTableLanguages as jest.Mock).mockResolvedValue(null);
  (saveValidatedLookupTableToDatabase as jest.Mock).mockResolvedValue(undefined);
  (convertLookupTableToSW3Format as jest.Mock).mockResolvedValue(undefined);
  (createPostgresValidationSchema as jest.Mock).mockResolvedValue(undefined);
  (cleanUpPostgresValidationSchema as jest.Mock).mockResolvedValue(undefined);
  (previewGenerator as jest.Mock).mockReturnValue(mockViewDTO);
  (Dimension.findOneByOrFail as jest.Mock).mockResolvedValue(makeMockDimensionEntity());
});

// =============================================================================
// confirmJoinColumnAndValidateReferenceValues
// =============================================================================

describe('confirmJoinColumnAndValidateReferenceValues', () => {
  describe('successful join column discovery', () => {
    it('returns the join column when the first candidate has no missing values', async () => {
      mockQuery.mockResolvedValueOnce([{ total: 5 }]).mockResolvedValueOnce([]);

      const result = await confirmJoinColumnAndValidateReferenceValues(
        ['ref_code'],
        'dim_col',
        'mock-cube-id',
        'revision-1',
        'lookup_table'
      );

      expect(result).toBe('ref_code');
    });

    it('skips a column with missing values and returns the next with no missing values', async () => {
      mockQuery
        .mockResolvedValueOnce([{ total: 5 }])
        .mockResolvedValueOnce([{ fact_table_ref: 'A', lookup_table_ref: null }]) // bad_col: 1 missing
        .mockResolvedValueOnce([]); // ref_code: 0 missing → found

      const result = await confirmJoinColumnAndValidateReferenceValues(
        ['bad_col', 'ref_code'],
        'dim_col',
        'mock-cube-id',
        'revision-1',
        'lookup_table'
      );

      expect(result).toBe('ref_code');
    });

    it('continues to the next column when a query throws', async () => {
      mockQuery
        .mockResolvedValueOnce([{ total: 5 }])
        .mockRejectedValueOnce(new Error('db error'))
        .mockResolvedValueOnce([]);

      const result = await confirmJoinColumnAndValidateReferenceValues(
        ['bad_col', 'ref_code'],
        'dim_col',
        'mock-cube-id',
        'revision-1',
        'lookup_table'
      );

      expect(result).toBe('ref_code');
    });
  });

  describe('error cases', () => {
    it('throws with lookup_no_join_column when none of the fact table rows match', async () => {
      mockQuery.mockResolvedValueOnce([{ total: 3 }]).mockResolvedValueOnce([
        { fact_table_ref: 'A', lookup_table_ref: null },
        { fact_table_ref: 'B', lookup_table_ref: null },
        { fact_table_ref: 'C', lookup_table_ref: null }
      ]);

      await expect(
        confirmJoinColumnAndValidateReferenceValues(
          ['ref_code'],
          'dim_col',
          'mock-cube-id',
          'revision-1',
          'lookup_table'
        )
      ).rejects.toMatchObject({
        errorTag: 'errors.lookup_table_validation.lookup_no_join_column',
        extension: expect.objectContaining({ mismatch: true, totalNonMatching: 3 })
      });
    });

    it('throws with some_references_failed_to_match when only some rows are missing', async () => {
      mockQuery.mockResolvedValueOnce([{ total: 5 }]).mockResolvedValueOnce([
        { fact_table_ref: 'A', lookup_table_ref: null },
        { fact_table_ref: 'B', lookup_table_ref: null }
      ]);

      await expect(
        confirmJoinColumnAndValidateReferenceValues(
          ['ref_code'],
          'dim_col',
          'mock-cube-id',
          'revision-1',
          'lookup_table'
        )
      ).rejects.toMatchObject({
        errorTag: 'errors.lookup_table_validation.some_references_failed_to_match',
        extension: expect.objectContaining({
          mismatch: true,
          totalNonMatching: 2,
          nonMatchingDataTableValues: ['A', 'B']
        })
      });
    });

    it('uses the type parameter to build the error tag', async () => {
      mockQuery.mockResolvedValueOnce([{ total: 2 }]).mockResolvedValueOnce([
        { fact_table_ref: 'A', lookup_table_ref: null },
        { fact_table_ref: 'B', lookup_table_ref: null }
      ]);

      await expect(
        confirmJoinColumnAndValidateReferenceValues(['ref_code'], 'dim_col', 'mock-cube-id', 'revision-1', 'my_type')
      ).rejects.toMatchObject({ errorTag: 'errors.my_type_validation.lookup_no_join_column' });
    });
  });
});

// =============================================================================
// validateLookupTable
// =============================================================================

describe('validateLookupTable', () => {
  const validProtoTable = makeProtoLookupTable(['description_en', 'ref_code']);
  const dimension = makeDimension();

  describe('precondition checks', () => {
    it('returns 500 when the dimension fact table column is not found in the dataset', async () => {
      const dataset = makeDataset({ noDimensionColumn: true });

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dimension_validation.fact_table_column_not_found');
    });

    it('calls cleanUpPostgresValidationSchema when fact table column is not found', async () => {
      const dataset = makeDataset({ noDimensionColumn: true });

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(cleanUpPostgresValidationSchema).toHaveBeenCalledWith('mock-cube-id', 'lt-1');
    });

    it('returns 400 when no description columns are present in the lookup table', async () => {
      const dataset = makeDataset();
      const tableWithoutDescription = makeProtoLookupTable(['ref_code', 'lang_col']);

      const result = (await validateLookupTable(
        tableWithoutDescription,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.measure_validation.no_description_columns');
    });

    it('returns 400 when lookForPossibleJoinColumn throws', async () => {
      const dataset = makeDataset();
      (lookForPossibleJoinColumn as jest.Mock).mockImplementationOnce(() => {
        throw new Error('no join column found');
      });

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toBe('errors.lookup_validation.no_join_column');
    });
  });

  describe('join column validation errors', () => {
    it('returns an error when no fact table rows match any lookup column', async () => {
      const dataset = makeDataset();
      mockQuery.mockResolvedValueOnce([{ total: 2 }]).mockResolvedValueOnce([
        { fact_table_ref: 'A', lookup_table_ref: null },
        { fact_table_ref: 'B', lookup_table_ref: null }
      ]);

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toContain('lookup_no_join_column');
    });

    it('returns an error when only some fact table rows match the lookup column', async () => {
      const dataset = makeDataset();
      mockQuery
        .mockResolvedValueOnce([{ total: 5 }])
        .mockResolvedValueOnce([{ fact_table_ref: 'A', lookup_table_ref: null }]);

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(400);
      expect(result.errors[0].message.key).toContain('some_references_failed_to_match');
    });

    it('cleans up the mock cube after a join column error', async () => {
      const dataset = makeDataset();
      mockQuery
        .mockResolvedValueOnce([{ total: 1 }])
        .mockResolvedValueOnce([{ fact_table_ref: 'A', lookup_table_ref: null }]);

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(cleanUpPostgresValidationSchema).toHaveBeenCalledWith('mock-cube-id', 'lt-1');
    });
  });

  describe('post-validation errors', () => {
    it('returns 500 when convertLookupTableToSW3Format throws', async () => {
      const dataset = makeDataset();
      setupJoinColumnFoundMocks();
      (convertLookupTableToSW3Format as jest.Mock).mockRejectedValueOnce(new Error('conversion failed'));

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dimension_validation.lookup_table_loading_failed');
    });

    it('returns the language error when validateLookupTableLanguages returns an error DTO', async () => {
      const dataset = makeDataset();
      setupJoinColumnFoundMocks();
      const languageErr: ViewErrDTO = {
        status: 400,
        dataset_id: 'dataset-1',
        errors: [
          { field: 'language', message: { key: 'errors.lookup.language_mismatch', params: {} }, user_message: [] }
        ],
        extension: {}
      };
      (validateLookupTableLanguages as jest.Mock).mockResolvedValueOnce(languageErr);

      const result = await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(result).toBe(languageErr);
    });

    it('cleans up the mock cube when validateLookupTableLanguages returns an error', async () => {
      const dataset = makeDataset();
      setupJoinColumnFoundMocks();
      (validateLookupTableLanguages as jest.Mock).mockResolvedValueOnce({
        status: 400,
        dataset_id: 'dataset-1',
        errors: [],
        extension: {}
      });

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(cleanUpPostgresValidationSchema).toHaveBeenCalledWith('mock-cube-id', 'lt-1');
    });

    it('returns 500 when saveValidatedLookupTableToDatabase fails', async () => {
      const dataset = makeDataset();
      setupJoinColumnFoundMocks();
      (saveValidatedLookupTableToDatabase as jest.Mock).mockRejectedValueOnce(new Error('save failed'));

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.lookup_table_validation.unknown_error');
    });

    it('returns 500 when the preview query throws', async () => {
      const dataset = makeDataset();
      setupJoinColumnFoundMocks();
      mockQuery.mockRejectedValueOnce(new Error('preview query failed'));

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dimension.lookup_preview_generation_failed');
    });

    it('returns 500 when the preview result is empty', async () => {
      const dataset = makeDataset();
      setupJoinColumnFoundMocks();
      mockQuery.mockResolvedValueOnce([]); // empty preview

      const result = (await validateLookupTable(
        validProtoTable,
        dataset,
        draftRevision,
        dimension,
        'en-GB'
      )) as ViewErrDTO;

      expect(result.status).toBe(500);
      expect(result.errors[0].message.key).toBe('errors.dimension.lookup_preview_generation_failed');
    });
  });

  describe('happy path', () => {
    function setupHappyPathMocks() {
      setupJoinColumnFoundMocks();
      mockQuery.mockResolvedValueOnce([{ dim_col: 'val1', description: 'Desc 1' }]); // preview
    }

    it('returns a ViewDTO on full success', async () => {
      const dataset = makeDataset();
      setupHappyPathMocks();

      const result = await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(result).toBe(mockViewDTO);
    });

    it('saves the lookup table and dimension', async () => {
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(mockLookupTableSave).toHaveBeenCalled();
      expect(mockDimensionSave).toHaveBeenCalled();
    });

    it('calls saveValidatedLookupTableToDatabase with the cube id and lookup table id', async () => {
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(saveValidatedLookupTableToDatabase).toHaveBeenCalledWith('mock-cube-id', 'lt-1');
    });

    it('calls createPostgresValidationSchema with the correct arguments', async () => {
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(createPostgresValidationSchema).toHaveBeenCalledWith(
        'mock-cube-id',
        'revision-1',
        'dim_col',
        'lookup',
        'dt-1_tmp'
      );
    });

    it('calls cleanUpDimension when the dimension already has a lookup table', async () => {
      const { cleanUpDimension } = jest.requireMock('../../../src/services/dimension-processor');
      const dimensionWithLookup = makeDimension({ lookupTable: { id: 'old-lt' } });
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimensionWithLookup, 'en-GB');

      expect(cleanUpDimension).toHaveBeenCalledWith(dimensionWithLookup);
    });

    it('does not call cleanUpDimension when the dimension has no existing lookup table', async () => {
      const { cleanUpDimension } = jest.requireMock('../../../src/services/dimension-processor');
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(cleanUpDimension).not.toHaveBeenCalled();
    });

    it('passes the language argument to the preview query', async () => {
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'cy-GB');

      // Third mockQuery call is the preview SELECT; verify it was called with a query containing the language
      const previewCall = mockQuery.mock.calls[2][0] as string;
      expect(previewCall).toContain('cy-GB');
    });

    it('sets isSW2Format=false on the lookup table at the end regardless of table columns', async () => {
      const dataset = makeDataset();
      setupHappyPathMocks();

      await validateLookupTable(validProtoTable, dataset, draftRevision, dimension, 'en-GB');

      expect(mockLookupTable.isStatsWales2Format).toBe(false);
    });
  });
});
