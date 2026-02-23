import { FactTableValidationException } from '../../src/exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../../src/enums/fact-table-validation-exception-type';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { ValidatedSourceAssignment } from '../../src/services/dimension-processor';
import { Dataset } from '../../src/entities/dataset/dataset';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { Revision } from '../../src/entities/dataset/revision';
import { DataTable } from '../../src/entities/dataset/data-table';

// --- Mock setup ---

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

jest.mock('../../src/services/cube-builder', () => ({
  FACT_TABLE_NAME: 'fact_table'
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

// Import after mocks
import { factTableValidatorFromSource, sourceAssignmentFromFactTable } from '../../src/services/fact-table-validator';

// --- Helpers ---

function makeFactTableColumn(
  overrides: Partial<FactTableColumn> & { columnName: string; columnIndex: number }
): FactTableColumn {
  return {
    columnName: overrides.columnName,
    columnIndex: overrides.columnIndex,
    columnType: overrides.columnType ?? FactTableColumnType.Unknown,
    columnDatatype: overrides.columnDatatype ?? 'varchar',
    id: overrides.id ?? 'dataset-1'
  } as FactTableColumn;
}

function makeSourceAssignment(): ValidatedSourceAssignment {
  return {
    dataValues: { column_index: 1, column_name: 'data', column_type: FactTableColumnType.DataValues },
    measure: { column_index: 2, column_name: 'measure', column_type: FactTableColumnType.Measure },
    noteCodes: { column_index: 3, column_name: 'notes', column_type: FactTableColumnType.NoteCodes },
    dimensions: [{ column_index: 0, column_name: 'date', column_type: FactTableColumnType.Dimension }],
    ignore: []
  };
}

function makeDataset(overrides?: { noRevision?: boolean; noFactTable?: boolean; noDataTable?: boolean }): Dataset {
  const dataTable = overrides?.noDataTable ? undefined : ({ id: 'dt-1' } as DataTable);
  const revision = overrides?.noRevision
    ? undefined
    : ({
        id: 'rev-1',
        revisionIndex: 1,
        dataTable
      } as Revision);

  const factTable = overrides?.noFactTable
    ? undefined
    : ([
        makeFactTableColumn({ columnName: 'date', columnIndex: 0 }),
        makeFactTableColumn({ columnName: 'data', columnIndex: 1 }),
        makeFactTableColumn({ columnName: 'measure', columnIndex: 2 }),
        makeFactTableColumn({ columnName: 'notes', columnIndex: 3 })
      ] as FactTableColumn[]);

  return {
    id: 'dataset-1',
    draftRevision: revision,
    factTable
  } as Dataset;
}

// --- Tests ---

describe('factTableValidatorFromSource', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('precondition checks', () => {
    it('throws NoDraftRevision when dataset has no draft revision', async () => {
      const dataset = makeDataset({ noRevision: true });
      const sources = makeSourceAssignment();

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.NoDraftRevision);
      expect(error!.status).toBe(500);
      expect(mockQuery).not.toHaveBeenCalled();
    });

    it('throws when dataset has no fact table', async () => {
      const dataset = makeDataset({ noFactTable: true });
      const sources = makeSourceAssignment();

      await expect(factTableValidatorFromSource(dataset, sources)).rejects.toThrow(
        'Unable to find fact table for dataset'
      );
    });

    it('throws UnknownSourcesStillPresent when a column cannot be matched', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();
      // Remove the dimension so "date" column can't be matched
      sources.dimensions = [];

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.UnknownSourcesStillPresent);
      expect(error!.status).toBe(400);
    });

    it('throws NoDataValueColumn when no data values column is provided', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();
      sources.dataValues = null;
      // All columns still need to match, so add 'data' as ignore
      sources.ignore.push({ column_index: 1, column_name: 'data', column_type: FactTableColumnType.Ignore });

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.NoDataValueColumn);
      expect(error!.status).toBe(400);
    });
  });

  describe('numeric data values validation', () => {
    it('throws NonNumericDataValueColumn when non-numeric values are found', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // First query: numeric validation - returns failing values
      mockQuery.mockResolvedValueOnce([{ data_value: 'abc' }, { data_value: 'N/A' }]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.NonNumericDataValueColumn);
      expect(error!.status).toBe(400);
      expect(error!.data).toEqual([['abc', 'N/A']]);
      expect(error!.headers).toEqual([{ name: 'data', index: 1 }]);
    });

    it('throws UnknownError when numeric validation query fails', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      mockQuery.mockRejectedValueOnce(new Error('connection lost'));

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.UnknownError);
      expect(error!.status).toBe(500);
    });
  });

  describe('duplicate fact detection', () => {
    it('throws DuplicateFact when duplicate rows are found', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation - passes (empty array)
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK - succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK - fails with unique index error
      mockQuery.mockRejectedValueOnce(new Error('could not create unique index "fact_table_pkey"'));
      // Query 4: identifyDuplicateFacts query - returns duplicate rows
      mockQuery.mockResolvedValueOnce([
        { line_number: 2, date: '2015', data: '152', measure: '2', notes: '' },
        { line_number: 3, date: '2015', data: '152', measure: '2', notes: '' }
      ]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.DuplicateFact);
      expect(error!.status).toBe(400);
      expect(error!.data).toBeDefined();
      expect(error!.headers).toBeDefined();
    });

    it('falls through to incomplete facts when no duplicates found', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK fails with unique index error
      mockQuery.mockRejectedValueOnce(new Error('could not create unique index'));
      // Query 4: identifyDuplicateFacts - no duplicates found
      mockQuery.mockResolvedValueOnce([]);
      // Query 5: identifyIncompleteFacts - finds incomplete rows
      mockQuery.mockResolvedValueOnce([{ line_number: 3, date: null, data: '152', measure: '2', notes: '' }]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.IncompleteFact);
    });
  });

  describe('incomplete fact detection', () => {
    it('throws IncompleteFact when null values are found in PK columns', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK fails with null values
      mockQuery.mockRejectedValueOnce(new Error('column "date" of relation "fact_table" contains null values'));
      // Query 4: identifyIncompleteFacts - finds rows with nulls
      mockQuery.mockResolvedValueOnce([{ line_number: 3, date: null, data: '152', measure: '2', notes: '' }]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.IncompleteFact);
      expect(error!.status).toBe(400);
      expect(error!.data).toBeDefined();
    });

    it('throws generic IncompleteFact when null values error but no rows identified', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK fails with null values
      mockQuery.mockRejectedValueOnce(new Error('contains null values'));
      // Query 4: identifyIncompleteFacts - no rows found (edge case)
      mockQuery.mockResolvedValueOnce([]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.IncompleteFact);
    });
  });

  describe('primary key failure (unknown error)', () => {
    it('throws UnknownError when PK fails with an unexpected error message', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK fails with unexpected error
      mockQuery.mockRejectedValueOnce(new Error('some other PG error'));

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.UnknownError);
      expect(error!.status).toBe(500);
    });
  });

  describe('no data table on revision', () => {
    it('throws NoDataTable when revision has no data table', async () => {
      const dataset = makeDataset({ noDataTable: true });
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.NoDataTable);
      expect(error!.status).toBe(500);
    });
  });

  describe('note code validation', () => {
    it('throws BadNoteCodes when invalid note codes are found', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 4: get distinct note codes - returns bad ones
      mockQuery.mockResolvedValueOnce([{ codes: 'xyz' }, { codes: 'invalid,p' }]);
      // Query 5: get column names for error display
      mockQuery.mockResolvedValueOnce([
        { column_name: 'date' },
        { column_name: 'data' },
        { column_name: 'measure' },
        { column_name: 'notes' }
      ]);
      // Query 6: get rows with bad note codes
      mockQuery.mockResolvedValueOnce([
        { line_number: 1, date: '2015', data: '151', measure: '1', notes: 'xyz' },
        { line_number: 2, date: '2015', data: '152', measure: '2', notes: 'invalid,p' }
      ]);

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.BadNoteCodes);
      expect(error!.status).toBe(400);
      expect(error!.data).toBeDefined();
      expect(error!.headers).toBeDefined();
    });

    it('throws NoNoteCodes when note code extraction query fails', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 4: note code query fails
      mockQuery.mockRejectedValueOnce(new Error('query failed'));

      const error = await getValidationError(dataset, sources);
      expect(error).toBeDefined();
      expect(error!.type).toBe(FactTableValidationExceptionType.NoNoteCodes);
      expect(error!.status).toBe(500);
    });
  });

  describe('happy path', () => {
    it('completes without error when all validations pass', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 4: get distinct note codes - none (all empty)
      mockQuery.mockResolvedValueOnce([]);

      await expect(factTableValidatorFromSource(dataset, sources)).resolves.toBeUndefined();
    });

    it('passes with valid note codes', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      // Query 1: numeric validation passes
      mockQuery.mockResolvedValueOnce([]);
      // Query 2: drop PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 3: add PK succeeds
      mockQuery.mockResolvedValueOnce(undefined);
      // Query 4: get distinct note codes - valid ones
      mockQuery.mockResolvedValueOnce([{ codes: 'p' }, { codes: 'r,e' }]);

      await expect(factTableValidatorFromSource(dataset, sources)).resolves.toBeUndefined();
    });

    it('releases all query runners even on success', async () => {
      const dataset = makeDataset();
      const sources = makeSourceAssignment();

      mockQuery.mockResolvedValueOnce([]); // numeric validation
      mockQuery.mockResolvedValueOnce(undefined); // drop PK
      mockQuery.mockResolvedValueOnce(undefined); // add PK
      mockQuery.mockResolvedValueOnce([]); // note codes

      await factTableValidatorFromSource(dataset, sources);

      // 4 query runners created: numeric, dropPK, addPK, noteCodes
      expect(mockRelease).toHaveBeenCalledTimes(4);
    });
  });
});

// Helper to extract FactTableValidationException from a rejected promise
async function getValidationError(
  dataset: Dataset,
  sources: ValidatedSourceAssignment
): Promise<FactTableValidationException | undefined> {
  try {
    await factTableValidatorFromSource(dataset, sources);
    return undefined;
  } catch (err) {
    if (err instanceof FactTableValidationException) {
      return err;
    }
    // Re-throw unexpected errors
    throw err;
  }
}

describe('sourceAssignmentFromFactTable', () => {
  test('maps all column types correctly', () => {
    const columns: FactTableColumn[] = [
      makeFactTableColumn({ columnName: 'date', columnIndex: 0, columnType: FactTableColumnType.Dimension }),
      makeFactTableColumn({ columnName: 'data', columnIndex: 1, columnType: FactTableColumnType.DataValues }),
      makeFactTableColumn({ columnName: 'measure', columnIndex: 2, columnType: FactTableColumnType.Measure }),
      makeFactTableColumn({ columnName: 'notes', columnIndex: 3, columnType: FactTableColumnType.NoteCodes }),
      makeFactTableColumn({ columnName: 'extra', columnIndex: 4, columnType: FactTableColumnType.Ignore })
    ];

    const result = sourceAssignmentFromFactTable(columns);

    expect(result.dataValues).toEqual({
      column_index: 1,
      column_name: 'data',
      column_type: FactTableColumnType.DataValues
    });
    expect(result.measure).toEqual({
      column_index: 2,
      column_name: 'measure',
      column_type: FactTableColumnType.Measure
    });
    expect(result.noteCodes).toEqual({
      column_index: 3,
      column_name: 'notes',
      column_type: FactTableColumnType.NoteCodes
    });
    expect(result.dimensions).toHaveLength(1);
    expect(result.dimensions[0]).toEqual({
      column_index: 0,
      column_name: 'date',
      column_type: FactTableColumnType.Dimension
    });
    expect(result.ignore).toHaveLength(1);
    expect(result.ignore[0]).toEqual({
      column_index: 4,
      column_name: 'extra',
      column_type: FactTableColumnType.Ignore
    });
  });

  test('handles multiple dimensions', () => {
    const columns: FactTableColumn[] = [
      makeFactTableColumn({ columnName: 'area', columnIndex: 0, columnType: FactTableColumnType.Dimension }),
      makeFactTableColumn({ columnName: 'year', columnIndex: 1, columnType: FactTableColumnType.Dimension }),
      makeFactTableColumn({ columnName: 'data', columnIndex: 2, columnType: FactTableColumnType.DataValues }),
      makeFactTableColumn({ columnName: 'measure', columnIndex: 3, columnType: FactTableColumnType.Measure })
    ];

    const result = sourceAssignmentFromFactTable(columns);

    expect(result.dimensions).toHaveLength(2);
    expect(result.dimensions.map((d) => d.column_name)).toEqual(['area', 'year']);
  });

  test('returns nulls when columns are absent', () => {
    const columns: FactTableColumn[] = [
      makeFactTableColumn({ columnName: 'date', columnIndex: 0, columnType: FactTableColumnType.Dimension })
    ];

    const result = sourceAssignmentFromFactTable(columns);

    expect(result.dataValues).toBeNull();
    expect(result.measure).toBeNull();
    expect(result.noteCodes).toBeNull();
    expect(result.dimensions).toHaveLength(1);
    expect(result.ignore).toHaveLength(0);
  });

  test('handles empty column array', () => {
    const result = sourceAssignmentFromFactTable([]);

    expect(result.dataValues).toBeNull();
    expect(result.measure).toBeNull();
    expect(result.noteCodes).toBeNull();
    expect(result.dimensions).toHaveLength(0);
    expect(result.ignore).toHaveLength(0);
  });
});
