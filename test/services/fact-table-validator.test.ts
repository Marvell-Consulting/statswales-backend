import { QueryRunner } from 'typeorm';

import { factTableValidatorFromSource } from '../../src/services/fact-table-validator';
import { Dataset } from '../../src/entities/dataset/dataset';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { FactTableValidationException } from '../../src/exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../../src/enums/fact-table-validation-exception-type';
import { ValidatedSourceAssignment } from '../../src/services/dimension-processor';
import { Revision } from '../../src/entities/dataset/revision';
import { DataTable } from '../../src/entities/dataset/data-table';

// Mock dbManager — each test configures query behaviour via mockQueryFn
const mockQueryFn = jest.fn();
const mockReleaseFn = jest.fn();
jest.mock('../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: () => ({
      createQueryRunner: (): Partial<QueryRunner> => ({
        query: mockQueryFn,
        release: mockReleaseFn
      })
    })
  }
}));

// Let pg-format pass through so we can inspect generated SQL
jest.mock('../../src/services/cube-builder', () => ({
  FACT_TABLE_NAME: 'fact_table'
}));

function makeFactTableColumn(name: string, index: number, datatype = 'BIGINT'): FactTableColumn {
  const col = new FactTableColumn();
  col.id = 'dataset-1';
  col.columnName = name;
  col.columnIndex = index;
  col.columnDatatype = datatype;
  col.columnType = FactTableColumnType.Unknown;
  return col;
}

function makeDataset(factTableColumns: FactTableColumn[]): Dataset {
  const revision = new Revision();
  revision.id = 'revision-1';
  const dataTable = new DataTable();
  dataTable.id = 'data-table-1';
  revision.dataTable = dataTable;

  const dataset = {
    id: 'dataset-1',
    draftRevision: revision,
    factTable: factTableColumns
  } as unknown as Dataset;

  return dataset;
}

function makeSourceAssignment(overrides: Partial<ValidatedSourceAssignment> = {}): ValidatedSourceAssignment {
  return {
    dataValues: { column_index: 1, column_name: 'data', column_type: FactTableColumnType.DataValues },
    measure: { column_index: 2, column_name: 'measure', column_type: FactTableColumnType.Measure },
    noteCodes: { column_index: 3, column_name: 'notes', column_type: FactTableColumnType.NoteCodes },
    dimensions: [{ column_index: 0, column_name: 'date', column_type: FactTableColumnType.Dimension }],
    ignore: [],
    ...overrides
  };
}

describe('factTableValidatorFromSource', () => {
  const factTableColumns = [
    makeFactTableColumn('date', 0),
    makeFactTableColumn('data', 1),
    makeFactTableColumn('measure', 2),
    makeFactTableColumn('notes', 3, 'VARCHAR')
  ];

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('duplicate fact detection', () => {
    it('should throw DuplicateFact when the primary key constraint fails and duplicates are found', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      // Call sequence:
      // 1. numeric validation query — no bad values
      // 2. drop PK constraint — succeeds
      // 3. add PK constraint — fails with "could not create unique index"
      // 4. identify duplicates query — returns duplicate rows
      mockQueryFn
        .mockResolvedValueOnce([]) // numeric validation: no non-numeric values
        .mockResolvedValueOnce(undefined) // drop PK
        .mockRejectedValueOnce(new Error('could not create unique index "fact_table_pkey"')) // add PK fails
        .mockResolvedValueOnce([
          // identify duplicates returns rows
          { line_number: 2, date: '2015', data: '152', measure: '2', notes: '' },
          { line_number: 3, date: '2015', data: '152', measure: '2', notes: '' }
        ]);

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.DuplicateFact);
        expect(error.status).toBe(400);
        expect(error.data).toHaveLength(2);
        expect(error.headers).toBeDefined();
        expect(error.headers.length).toBeGreaterThan(0);
      }
    });

    it('should include line numbers and all columns in duplicate fact error data', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      mockQueryFn
        .mockResolvedValueOnce([]) // numeric validation
        .mockResolvedValueOnce(undefined) // drop PK
        .mockRejectedValueOnce(new Error('could not create unique index "fact_table_pkey"'))
        .mockResolvedValueOnce([
          { line_number: 3, date: '2015', data: '152', measure: '2', notes: '' },
          { line_number: 4, date: '2015', data: '152', measure: '2', notes: '' }
        ]);

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.DuplicateFact);
        // Verify headers include line_number column
        const headerNames = error.headers.map((h) => h.name);
        expect(headerNames).toContain('line_number');
        // Verify data rows contain the duplicate values
        expect(error.data[0]).toContain(3);
        expect(error.data[1]).toContain(4);
      }
    });

    it('should generate a valid SQL query with subquery aliases for duplicate detection', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      mockQueryFn
        .mockResolvedValueOnce([]) // numeric validation
        .mockResolvedValueOnce(undefined) // drop PK
        .mockRejectedValueOnce(new Error('could not create unique index "fact_table_pkey"'))
        .mockResolvedValueOnce([]); // identify duplicates — no rows (edge case)

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
      } catch {
        // may throw UnknownError if no duplicates found — that's fine for this test
      }

      // The 4th query call is the duplicate identification query
      const duplicateQuery: string = mockQueryFn.mock.calls[3][0];
      // Verify subquery aliases are present (the bug fix)
      expect(duplicateQuery).toMatch(/\)\s*AS\s+ft\b/i);
      expect(duplicateQuery).toMatch(/\)\s*AS\s+dups\b/i);
    });
  });

  describe('incomplete fact detection', () => {
    it('should throw IncompleteFact when PK constraint fails due to null values', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      mockQueryFn
        .mockResolvedValueOnce([]) // numeric validation
        .mockResolvedValueOnce(undefined) // drop PK
        .mockRejectedValueOnce(new Error('column "date" of relation "fact_table" contains null values')) // add PK fails
        .mockResolvedValueOnce([
          // identify incomplete facts
          { line_number: 2, date: null, data: '100', measure: '1', notes: '' }
        ]);

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.IncompleteFact);
        expect(error.status).toBe(400);
        expect(error.data).toHaveLength(1);
      }
    });

    it('should generate a valid SQL query with subquery alias for incomplete fact detection', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      mockQueryFn
        .mockResolvedValueOnce([]) // numeric validation
        .mockResolvedValueOnce(undefined) // drop PK
        .mockRejectedValueOnce(new Error('column "date" of relation "fact_table" contains null values'))
        .mockResolvedValueOnce([]); // no incomplete facts found

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
      } catch {
        // expected
      }

      const incompleteQuery: string = mockQueryFn.mock.calls[3][0];
      expect(incompleteQuery).toMatch(/\)\s*AS\s+ft\b/i);
    });
  });

  describe('successful validation', () => {
    it('should not throw when the primary key constraint succeeds (no duplicates)', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      mockQueryFn
        .mockResolvedValueOnce([]) // numeric validation: all values valid
        .mockResolvedValueOnce(undefined) // drop PK
        .mockResolvedValueOnce(undefined) // add PK succeeds
        .mockResolvedValueOnce([]); // note codes validation

      await expect(factTableValidatorFromSource(dataset, sourceAssignment)).resolves.not.toThrow();
    });
  });

  describe('precondition checks', () => {
    it('should throw NoDraftRevision when dataset has no draft revision', async () => {
      const dataset = { id: 'dataset-1', draftRevision: undefined, factTable: factTableColumns } as unknown as Dataset;
      const sourceAssignment = makeSourceAssignment();

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.NoDraftRevision);
      }
    });

    it('should throw NoDataValueColumn when no data values column is assigned', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment({
        dataValues: null,
        ignore: [{ column_index: 1, column_name: 'data', column_type: FactTableColumnType.Ignore }]
      });

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.NoDataValueColumn);
      }
    });

    it('should throw UnknownSourcesStillPresent when a column has no source assignment', async () => {
      const dataset = makeDataset(factTableColumns);
      // Source assignment is missing the 'notes' column entirely
      const sourceAssignment = makeSourceAssignment({ noteCodes: null });

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.UnknownSourcesStillPresent);
      }
    });

    it('should throw NonNumericDataValueColumn when data values contain non-numeric values', async () => {
      const dataset = makeDataset(factTableColumns);
      const sourceAssignment = makeSourceAssignment();

      // numeric validation returns bad values
      mockQueryFn.mockResolvedValueOnce([{ data_value: 'abc' }, { data_value: 'n/a' }]);

      try {
        await factTableValidatorFromSource(dataset, sourceAssignment);
        fail('Expected FactTableValidationException to be thrown');
      } catch (err) {
        const error = err as FactTableValidationException;
        expect(error.type).toBe(FactTableValidationExceptionType.NonNumericDataValueColumn);
        expect(error.status).toBe(400);
      }
    });
  });
});
