// --- Mock setup (must come before imports) ---

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    trace: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

jest.mock('../../../src/utils/performance-reporting', () => ({
  performanceReporting: jest.fn()
}));

jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: jest.fn()
  }
}));

jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {}
}));

jest.mock('../../../src/repositories/revision', () => ({
  RevisionRepository: {}
}));

jest.mock('../../../src/entities/dataset/build-log', () => ({
  BuildLog: {
    startBuild: jest.fn(),
    findOneByOrFail: jest.fn()
  }
}));

jest.mock('../../../src/entities/query-store', () => ({
  QueryStore: {}
}));

// --- Imports after mocks ---

import {
  makeCubeSafeString,
  createValidationTableQuery,
  setupValidationTableFromDataset,
  loadTableDataIntoFactTableFromPostgresStatement,
  cleanupNotesCodeColumn,
  dataTableActions,
  createPrimaryKeyOnFactTable,
  measureTableCreateStatement,
  createMeasureLookupTable,
  createLookupTableDimension,
  setupCubeBuilder,
  setupLookupTableDimension,
  updateFilterTableCounts,
  FACT_TABLE_NAME,
  FILTER_TABLE_NAME,
  METADATA_TABLE_NAME,
  FILTER_TABLE_VERSION
} from '../../../src/services/cube-builder';

import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { BuildStage } from '../../../src/enums/build-stage';
import { DataTableAction } from '../../../src/enums/data-table-action';
import { DisplayType } from '../../../src/enums/display-type';
import { DimensionType } from '../../../src/enums/dimension-type';
import { Locale } from '../../../src/enums/locale';
import { FactTableColumn } from '../../../src/entities/dataset/fact-table-column';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Measure } from '../../../src/entities/dataset/measure';
import { Dimension } from '../../../src/entities/dataset/dimension';
import { DataTable } from '../../../src/entities/dataset/data-table';
import { DataTableDescription } from '../../../src/entities/dataset/data-table-description';
import { MeasureRow } from '../../../src/entities/dataset/measure-row';
import { LookupTable } from '../../../src/entities/dataset/lookup-table';

// --- Fixture factories ---

function makeCol(
  columnName: string,
  columnIndex: number,
  columnType: FactTableColumnType = FactTableColumnType.Unknown,
  columnDatatype = 'TEXT'
): FactTableColumn {
  return { id: `col-${columnName}`, columnName, columnIndex, columnType, columnDatatype } as FactTableColumn;
}

function makeMeasureRow(overrides: {
  reference: string;
  language: string;
  description: string;
  format?: DisplayType;
  notes?: string | null;
  sortOrder?: number | null;
  decimal?: number | null;
  measureType?: string | null;
  hierarchy?: string | null;
}): MeasureRow {
  return {
    reference: overrides.reference,
    language: overrides.language,
    description: overrides.description,
    format: overrides.format ?? DisplayType.String,
    notes: overrides.notes ?? null,
    sortOrder: overrides.sortOrder ?? null,
    decimal: overrides.decimal ?? null,
    measureType: overrides.measureType ?? null,
    hierarchy: overrides.hierarchy ?? null
  } as MeasureRow;
}

function makeDataTableDescription(columnName: string, factTableColumn: string, columnIndex = 0): DataTableDescription {
  return {
    id: 'dtd-1',
    columnName,
    factTableColumn,
    columnIndex,
    columnDatatype: 'TEXT'
  } as DataTableDescription;
}

function makeDataTable(action: DataTableAction, descriptions: DataTableDescription[], id = 'data-table-1'): DataTable {
  return { action, dataTableDescriptions: descriptions, id } as DataTable;
}

function makeDimension(type: DimensionType, factTableColumn: string): Dimension {
  return {
    type,
    factTableColumn,
    metadata: [],
    lookupTable: { id: 'lookup-table-uuid' } as LookupTable
  } as unknown as Dimension;
}

function makeSetupLookupArgs() {
  const locales = [Locale.EnglishGb, Locale.WelshGb];
  return {
    coreCubeViewSelectBuilder: new Map(locales.map((l) => [l, [] as string[]])),
    columnNames: new Map(locales.map((l) => [l, new Set<string>()])),
    indexColumns: new Map(locales.map((l) => [l, [] as string[]])),
    joinStatements: [] as string[],
    orderByStatements: [] as string[],
    viewConfig: []
  };
}

function makeDataset(overrides?: {
  measure?: { factTableColumn: string };
  dimensions?: Array<{ factTableColumn: string }>;
  factTable?: FactTableColumn[];
}): Dataset {
  const measure = overrides?.measure ? ({ factTableColumn: overrides.measure.factTableColumn } as Measure) : undefined;
  const dimensions =
    overrides?.dimensions?.map((d) => ({ factTableColumn: d.factTableColumn, metadata: [] }) as unknown as Dimension) ??
    [];

  return {
    id: 'dataset-1',
    measure,
    dimensions,
    factTable: overrides?.factTable
  } as unknown as Dataset;
}

// --- Tests ---

describe('FACT_TABLE_NAME constant', () => {
  it('equals "fact_table"', () => {
    expect(FACT_TABLE_NAME).toBe('fact_table');
  });
});

// ===========================================================================
describe('makeCubeSafeString', () => {
  it('lowercases input', () => {
    expect(makeCubeSafeString('YearCode')).toBe('yearcode');
  });

  it('replaces spaces with underscores', () => {
    expect(makeCubeSafeString('hello world')).toBe('hello_world');
  });

  it('preserves existing underscores', () => {
    expect(makeCubeSafeString('all_good')).toBe('all_good');
  });

  it('strips digits', () => {
    expect(makeCubeSafeString('Col2023')).toBe('col');
  });

  it('strips digits but keeps letters and underscored space', () => {
    expect(makeCubeSafeString('Column 1')).toBe('column_');
  });

  it('strips hyphens', () => {
    expect(makeCubeSafeString('has-hyphens')).toBe('hashyphens');
  });

  it('strips dots', () => {
    expect(makeCubeSafeString('WITH.DOTS')).toBe('withdots');
  });

  it('returns empty string for empty input', () => {
    expect(makeCubeSafeString('')).toBe('');
  });

  it('strips all non-alpha chars leaving only underscores', () => {
    // spaces become _ (kept), digits stripped, letters kept
    expect(makeCubeSafeString('a1b2c3')).toBe('abc');
  });
});

// ===========================================================================
describe('createValidationTableQuery', () => {
  it('always targets VALIDATION_TABLE_NAME, not an arbitrary table', () => {
    const sql = createValidationTableQuery('build-123');
    expect(sql).toContain('validation_table');
  });

  it('defines reference and fact_table_column as TEXT columns', () => {
    const sql = createValidationTableQuery('build-123');
    expect(sql).toContain('reference TEXT');
    expect(sql).toContain('fact_table_column TEXT');
  });
});

// ===========================================================================
describe('setupValidationTableFromDataset', () => {
  const buildId = 'build-abc';

  it('returns the correct buildStage', () => {
    const dataset = makeDataset({ measure: { factTableColumn: 'measure_col' }, dimensions: [] });
    const result = setupValidationTableFromDataset(buildId, dataset);
    expect(result.buildStage).toBe(BuildStage.ValidationTableBuild);
  });

  it('starts with BEGIN TRANSACTION and ends with COMMIT', () => {
    const dataset = makeDataset({ measure: { factTableColumn: 'measure_col' }, dimensions: [] });
    const result = setupValidationTableFromDataset(buildId, dataset);
    expect(result.statements[0]).toBe('BEGIN TRANSACTION;');
    expect(result.statements.at(-1)).toBe('COMMIT;');
  });

  it('includes CREATE validation_table statement', () => {
    const dataset = makeDataset({ measure: { factTableColumn: 'measure_col' }, dimensions: [] });
    const result = setupValidationTableFromDataset(buildId, dataset);
    const hasCreate = result.statements.some(
      (s) => s.includes('validation_table') && s.toUpperCase().includes('CREATE TABLE')
    );
    expect(hasCreate).toBe(true);
  });

  it('includes an INSERT INTO validation_table with measure column', () => {
    const dataset = makeDataset({ measure: { factTableColumn: 'measure_col' }, dimensions: [] });
    const result = setupValidationTableFromDataset(buildId, dataset);
    const insertStmt = result.statements.find((s) => s.includes('validation_table') && s.includes('INSERT INTO'));
    expect(insertStmt).toBeDefined();
    expect(insertStmt).toContain('measure_col'); // lowercase/underscore identifiers are not quoted
    expect(insertStmt).not.toContain('UNION ALL');
  });

  it('adds UNION ALL when multiple dimensions are present', () => {
    const dataset = makeDataset({
      measure: { factTableColumn: 'measure_col' },
      dimensions: [{ factTableColumn: 'geo_col' }, { factTableColumn: 'time_col' }]
    });
    const result = setupValidationTableFromDataset(buildId, dataset);
    const insertStmt = result.statements.find((s) => s.includes('validation_table') && s.includes('INSERT INTO'));
    expect(insertStmt).toContain('UNION ALL');
    expect(insertStmt).toContain('geo_col');
    expect(insertStmt).toContain('time_col');
  });

  it('works with dimensions only and no measure', () => {
    const dataset = makeDataset({ dimensions: [{ factTableColumn: 'region' }] });
    const result = setupValidationTableFromDataset(buildId, dataset);
    const insertStmt = result.statements.find((s) => s.includes('validation_table') && s.includes('INSERT INTO'));
    expect(insertStmt).toBeDefined();
    expect(insertStmt).toContain('region');
    expect(insertStmt).not.toContain('UNION ALL');
  });

  it('includes an INSERT for fact_count in metadata', () => {
    const dataset = makeDataset({ measure: { factTableColumn: 'measure_col' }, dimensions: [] });
    const result = setupValidationTableFromDataset(buildId, dataset);
    const factCountStmt = result.statements.find((s) => s.includes('fact_count') && s.includes('COUNT(*)'));
    expect(factCountStmt).toBeDefined();
  });
});

// ===========================================================================
describe('loadTableDataIntoFactTableFromPostgresStatement', () => {
  it('projects every column in factTableDef — no more, no less', () => {
    const cols = ['year', 'region', 'value', 'notes'];
    const sql = loadTableDataIntoFactTableFromPostgresStatement('b1', cols, 'fact_table', 'dt-1');
    for (const col of cols) {
      expect(sql).toContain(col);
    }
  });

  it('reads from the data_tables schema using the supplied dataTableId', () => {
    const sql = loadTableDataIntoFactTableFromPostgresStatement('b1', ['col_a'], 'fact_table', 'specific-uuid');
    // source is always data_tables regardless of buildId
    expect(sql).toContain('data_tables');
    expect(sql).toContain('specific-uuid');
  });

  it('writes to the factTableName argument, not a hardcoded name', () => {
    const sql1 = loadTableDataIntoFactTableFromPostgresStatement('b1', ['col_a'], 'fact_table', 'dt-1');
    const sql2 = loadTableDataIntoFactTableFromPostgresStatement('b1', ['col_a'], 'staging_table', 'dt-1');
    expect(sql1).toContain('fact_table');
    expect(sql2).toContain('staging_table');
    expect(sql2).not.toContain('fact_table');
  });
});

// ===========================================================================
describe('cleanupNotesCodeColumn', () => {
  it('uses the notesCodeColumn name in both SET and WHERE (same column, not two different ones)', () => {
    const col = makeCol('note_codes', 0, FactTableColumnType.NoteCodes);
    const sql = cleanupNotesCodeColumn('b1', col);
    // the column being nulled and the column being matched must be identical
    const occurrences = (sql.match(/note_codes/g) ?? []).length;
    expect(occurrences).toBeGreaterThanOrEqual(2);
  });

  it('sets the column to NULL where its value is an empty string', () => {
    const col = makeCol('note_codes', 0, FactTableColumnType.NoteCodes);
    const sql = cleanupNotesCodeColumn('b1', col);
    expect(sql).toMatch(/SET\s+\S+\s*=\s*NULL/i);
    expect(sql).toContain("= ''");
  });

  it('always targets FACT_TABLE_NAME, never another table', () => {
    const col = makeCol('note_codes', 0, FactTableColumnType.NoteCodes);
    const sql = cleanupNotesCodeColumn('any-build', col);
    expect(sql).toContain('fact_table');
  });
});

// ===========================================================================
describe('createPrimaryKeyOnFactTable', () => {
  it('produces an ALTER TABLE … ADD PRIMARY KEY statement targeting fact_table', () => {
    const sql = createPrimaryKeyOnFactTable('b1', ['year']);
    expect(sql).toMatch(/ALTER TABLE/i);
    expect(sql).toMatch(/ADD PRIMARY KEY/i);
    expect(sql).toContain('fact_table');
  });

  it('includes every compositeKey column and nothing extra', () => {
    const key = ['year', 'region', 'measure'];
    const sql = createPrimaryKeyOnFactTable('b1', key);
    for (const col of key) {
      expect(sql).toContain(col);
    }
    // DataValues / NoteCodes columns must never appear in the primary key
    expect(sql).not.toContain('value');
    expect(sql).not.toContain('notes');
  });
});

// ===========================================================================
describe('measureTableCreateStatement', () => {
  it('returns a CREATE TABLE with reference and hierarchy using the provided joinColumnType', () => {
    const sql = measureTableCreateStatement('TEXT');
    expect(sql.trim().toUpperCase()).toMatch(/^CREATE TABLE/);
    expect(sql).toContain('reference TEXT');
    expect(sql).toContain('hierarchy TEXT');
    expect(sql).toContain('language TEXT');
    expect(sql).toContain('description TEXT');
    expect(sql).toContain('sort_order TEXT');
    expect(sql).toContain('format TEXT');
    expect(sql).toContain('decimals INTEGER');
    expect(sql).toContain('measure_type TEXT');
  });

  it('uses joinColumnType for both reference and hierarchy columns', () => {
    const sql = measureTableCreateStatement('VARCHAR');
    const refCount = (sql.match(/reference VARCHAR/g) ?? []).length;
    const hierCount = (sql.match(/hierarchy VARCHAR/g) ?? []).length;
    expect(refCount).toBe(1);
    expect(hierCount).toBe(1);
  });

  it('defaults to the table name "measure" when no buildId is provided', () => {
    const sql = measureTableCreateStatement('TEXT');
    expect(sql).toContain('measure');
    // without a buildId there should be no schema-qualified (dotted) table reference
    expect(sql).not.toMatch(/\w+\.\w*measure/);
  });

  it('schema-qualifies the table with buildId when provided', () => {
    const sql = measureTableCreateStatement('TEXT', 'mybuild');
    // the buildId must appear as a schema prefix before the table name
    expect(sql).toMatch(/mybuild[^.]*\..*measure/i);
  });

  it('uses the supplied tableName instead of the default when buildId is given', () => {
    const sql1 = measureTableCreateStatement('TEXT', 'b1');
    const sql2 = measureTableCreateStatement('TEXT', 'b1', 'custom_measure');
    expect(sql1).toContain('measure');
    expect(sql2).toContain('custom_measure');
  });
});

// ===========================================================================
describe('createMeasureLookupTable', () => {
  const measureCol = makeCol('measure', 0, FactTableColumnType.Measure, 'TEXT');

  it('returns only the CREATE TABLE statement when measureTable is empty', () => {
    const stmts = createMeasureLookupTable('b1', measureCol, []);
    expect(stmts).toHaveLength(1);
    expect(stmts[0].trim().toUpperCase()).toMatch(/^CREATE TABLE/);
  });

  it('returns CREATE TABLE + one INSERT per row', () => {
    const rows = [
      makeMeasureRow({ reference: 'ref1', language: 'en-GB', description: 'English label' }),
      makeMeasureRow({ reference: 'ref1', language: 'cy-GB', description: 'Welsh label' })
    ];
    const stmts = createMeasureLookupTable('b1', measureCol, rows);
    expect(stmts).toHaveLength(3); // 1 CREATE + 2 INSERTs
  });

  it('inserts NULL for null optional fields', () => {
    const row = makeMeasureRow({
      reference: 'ref1',
      language: 'en-GB',
      description: 'label',
      notes: null,
      sortOrder: null,
      decimal: null,
      measureType: null,
      hierarchy: null
    });
    const stmts = createMeasureLookupTable('b1', measureCol, [row]);
    const insert = stmts[1];
    expect(insert).toContain('INSERT INTO');
    expect(insert).toContain("'ref1'");
    expect(insert).toContain("'en-gb'"); // language is lowercased
    expect(insert).toContain("'label'");
    // null optional fields become SQL NULL
    expect(insert).toContain('NULL');
  });

  it('lowercases the language value', () => {
    const row = makeMeasureRow({ reference: 'r1', language: 'EN-GB', description: 'test' });
    const stmts = createMeasureLookupTable('b1', measureCol, [row]);
    expect(stmts[1]).toContain("'en-gb'");
    expect(stmts[1]).not.toContain("'EN-GB'");
  });

  it('inserts actual values for populated optional fields', () => {
    const row = makeMeasureRow({
      reference: 'ref1',
      language: 'en-GB',
      description: 'label',
      notes: 'some note',
      sortOrder: 5,
      decimal: 2,
      measureType: 'total',
      hierarchy: 'parent'
    });
    const stmts = createMeasureLookupTable('b1', measureCol, [row]);
    const insert = stmts[1];
    expect(insert).toContain("'some note'");
    expect(insert).toContain(',5,'); // pg-format outputs numbers bare (not quoted), no spaces in value list
    expect(insert).toContain("'total'");
    expect(insert).toContain("'parent'");
  });
});

// ===========================================================================
describe('createLookupTableDimension', () => {
  it('derives the dest table name from factTableColumn.columnName, not dimension.factTableColumn', () => {
    // dimension.factTableColumn and factTableColumn.columnName are intentionally different
    const dimension = {
      factTableColumn: 'geo',
      lookupTable: { id: 'geo-lookup-id' } as LookupTable
    } as Dimension;
    const factTableCol = makeCol('Year2023Code', 0, FactTableColumnType.Dimension);

    const sql = createLookupTableDimension('b1', dimension, factTableCol);
    // makeCubeSafeString('Year2023Code') = 'yearcode' → table should be 'yearcode_lookup'
    expect(sql).toContain('yearcode_lookup');
    // must NOT use dimension.factTableColumn ('geo') as the table name
    expect(sql).not.toContain('geo_lookup');
  });

  it('reads from the lookup_tables schema using the lookupTable.id', () => {
    const dimension = {
      factTableColumn: 'region',
      lookupTable: { id: 'specific-lookup-uuid' } as LookupTable
    } as Dimension;
    const factTableCol = makeCol('region', 0, FactTableColumnType.Dimension);

    const sql = createLookupTableDimension('b1', dimension, factTableCol);
    expect(sql).toContain('lookup_tables');
    expect(sql).toContain('specific-lookup-uuid');
  });

  it('copies the full lookup table with SELECT *', () => {
    const dimension = {
      factTableColumn: 'region',
      lookupTable: { id: 'some-id' } as LookupTable
    } as Dimension;
    const factTableCol = makeCol('region', 0, FactTableColumnType.Dimension);

    const sql = createLookupTableDimension('b1', dimension, factTableCol);
    expect(sql).toMatch(/SELECT \*/i);
  });
});

// ===========================================================================
describe('setupCubeBuilder', () => {
  it('throws when dataset has no factTable', () => {
    const dataset = makeDataset();
    expect(() => setupCubeBuilder(dataset, 'b1')).toThrow('Unable to find fact table for dataset dataset-1');
  });

  it('identifies DataValues, NoteCodes, and Measure columns', () => {
    const factTable = [
      makeCol('year', 0, FactTableColumnType.Dimension),
      makeCol('value', 1, FactTableColumnType.DataValues),
      makeCol('notes', 2, FactTableColumnType.NoteCodes),
      makeCol('measure', 3, FactTableColumnType.Measure)
    ];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'b1');

    expect(info.dataValuesColumn?.columnName).toBe('value');
    expect(info.notesCodeColumn?.columnName).toBe('notes');
    expect(info.measureColumn?.columnName).toBe('measure');
  });

  it('returns undefined for optional columns when absent', () => {
    const factTable = [makeCol('year', 0, FactTableColumnType.Dimension)];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'b1');

    expect(info.dataValuesColumn).toBeUndefined();
    expect(info.notesCodeColumn).toBeUndefined();
    expect(info.measureColumn).toBeUndefined();
  });

  it('includes only Dimension, Measure, and Time columns in compositeKey', () => {
    const factTable = [
      makeCol('year', 0, FactTableColumnType.Time),
      makeCol('region', 1, FactTableColumnType.Dimension),
      makeCol('value', 2, FactTableColumnType.DataValues),
      makeCol('notes', 3, FactTableColumnType.NoteCodes),
      makeCol('measure', 4, FactTableColumnType.Measure)
    ];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'b1');

    expect(info.compositeKey).toEqual(expect.arrayContaining(['year', 'region', 'measure']));
    expect(info.compositeKey).not.toContain('value');
    expect(info.compositeKey).not.toContain('notes');
    expect(info.compositeKey).toHaveLength(3);
  });

  it('includes all columns in factTableDef', () => {
    const factTable = [
      makeCol('year', 0, FactTableColumnType.Time),
      makeCol('value', 1, FactTableColumnType.DataValues),
      makeCol('notes', 2, FactTableColumnType.NoteCodes)
    ];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'b1');

    expect(info.factTableDef).toEqual(expect.arrayContaining(['year', 'value', 'notes']));
    expect(info.factTableDef).toHaveLength(3);
  });

  it('substitutes DOUBLE with DOUBLE PRECISION in the CREATE TABLE DDL', () => {
    const factTable = [
      makeCol('year', 0, FactTableColumnType.Dimension, 'TEXT'),
      makeCol('value', 1, FactTableColumnType.DataValues, 'DOUBLE')
    ];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'b1');

    expect(info.factTableCreationQuery).toContain('DOUBLE PRECISION');
    expect(info.factTableCreationQuery).not.toMatch(/"value" DOUBLE[^$\s]/);
  });

  it('sorts columns by columnIndex in the CREATE TABLE DDL', () => {
    // Provide columns out of order — DDL should reflect columnIndex order
    const factTable = [
      makeCol('c_third', 2, FactTableColumnType.Dimension),
      makeCol('a_first', 0, FactTableColumnType.Dimension),
      makeCol('b_second', 1, FactTableColumnType.DataValues)
    ];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'b1');

    const ddl = info.factTableCreationQuery;
    // plain lowercase identifiers are not quoted by pgformat %I
    const firstPos = ddl.indexOf('a_first');
    const secondPos = ddl.indexOf('b_second');
    const thirdPos = ddl.indexOf('c_third');
    expect(firstPos).toBeLessThan(secondPos);
    expect(secondPos).toBeLessThan(thirdPos);
  });

  it('produces a schema-qualified CREATE TABLE query using buildId', () => {
    const factTable = [makeCol('year', 0, FactTableColumnType.Dimension)];
    const dataset = makeDataset({ factTable });
    const info = setupCubeBuilder(dataset, 'my-build-id');

    // hyphenated buildId gets quoted; plain fact_table does not
    expect(info.factTableCreationQuery).toContain('"my-build-id".fact_table');
  });
});

// ===========================================================================
describe('dataTableActions', () => {
  const buildId = 'b1';
  const notesCol = makeCol('note_codes', 3, FactTableColumnType.NoteCodes);
  const dataValuesCol = makeCol('data_value', 2, FactTableColumnType.DataValues);
  const yearCol = makeCol('year', 0, FactTableColumnType.Dimension);
  const factIdentifiers = [yearCol];
  const factTableDef = ['year', 'data_value', 'note_codes'];

  const matchingDescriptions = [makeDataTableDescription('year', 'year', 0)];

  beforeEach(() => {
    jest
      .spyOn(globalThis.crypto, 'randomUUID')
      .mockReturnValue('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' as ReturnType<typeof crypto.randomUUID>);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('ReplaceAll', () => {
    it('returns 2 statements: DELETE + INSERT', () => {
      const dataTable = makeDataTable(DataTableAction.ReplaceAll, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts).toHaveLength(2);
    });

    it('first statement is a DELETE from fact_table', () => {
      const dataTable = makeDataTable(DataTableAction.ReplaceAll, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[0]).toContain('DELETE FROM');
      expect(stmts[0]).toContain('b1.fact_table');
    });

    it('second statement is an INSERT INTO fact_table', () => {
      const dataTable = makeDataTable(DataTableAction.ReplaceAll, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[1]).toContain('INSERT INTO');
      expect(stmts[1]).toContain('b1.fact_table');
    });
  });

  describe('Add', () => {
    it('returns 4 statements: 3 strip-existing-codes + 1 INSERT', () => {
      const dataTable = makeDataTable(DataTableAction.Add, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts).toHaveLength(4);
    });

    it('first three statements UPDATE fact_table to strip note codes', () => {
      const dataTable = makeDataTable(DataTableAction.Add, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[0]).toContain('UPDATE');
      expect(stmts[1]).toContain('UPDATE');
      expect(stmts[2]).toContain('UPDATE');
    });

    it('last statement is an INSERT INTO fact_table', () => {
      const dataTable = makeDataTable(DataTableAction.Add, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[3]).toContain('INSERT INTO');
    });
  });

  describe('Revise', () => {
    it('returns 12 statements', () => {
      const dataTable = makeDataTable(DataTableAction.Revise, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      // CREATE TEMP(1) + finaliseValues(2) + strip(3) + updatePF(2) + fixNoteCodes(2) + updateFacts(1) + drop(1)
      expect(stmts).toHaveLength(12);
    });

    it('starts with CREATE TEMPORARY TABLE using the action UUID', () => {
      const dataTable = makeDataTable(DataTableAction.Revise, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[0]).toContain('CREATE TEMPORARY TABLE');
      expect(stmts[0]).toContain('"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"');
    });

    it('ends with DROP TABLE of the temp table', () => {
      const dataTable = makeDataTable(DataTableAction.Revise, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts.at(-1)).toContain('DROP TABLE');
      expect(stmts.at(-1)).toContain('"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"');
    });
  });

  describe('AddRevise', () => {
    it('returns 14 statements', () => {
      const dataTable = makeDataTable(DataTableAction.AddRevise, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      // CREATE TEMP(1) + finaliseValues(2) + strip(3) + updatePF(2) + fixNoteCodes(2) +
      // updateFacts(1) + copyUpdateToFact(2) + drop(1)
      expect(stmts).toHaveLength(14);
    });

    it('starts with CREATE TEMPORARY TABLE', () => {
      const dataTable = makeDataTable(DataTableAction.AddRevise, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[0]).toContain('CREATE TEMPORARY TABLE');
    });

    it('ends with DROP TABLE', () => {
      const dataTable = makeDataTable(DataTableAction.AddRevise, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts.at(-1)).toContain('DROP TABLE');
    });
  });

  describe('Correction', () => {
    it('returns 3 statements: CREATE TEMP + UPDATE + DROP', () => {
      const dataTable = makeDataTable(DataTableAction.Correction, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts).toHaveLength(3);
    });

    it('creates a temp table from the data table', () => {
      const dataTable = makeDataTable(DataTableAction.Correction, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[0]).toContain('CREATE TEMPORARY TABLE');
      expect(stmts[0]).toContain('"data-table-1"');
    });

    it('includes an UPDATE on fact_table', () => {
      const dataTable = makeDataTable(DataTableAction.Correction, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[1]).toContain('UPDATE');
      expect(stmts[1]).toContain('b1.fact_table');
    });

    it('drops the temp table last', () => {
      const dataTable = makeDataTable(DataTableAction.Correction, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts[2]).toContain('DROP TABLE');
    });
  });

  describe('unknown action', () => {
    it('returns an empty array for an unknown action', () => {
      const dataTable = makeDataTable('unknown_action' as DataTableAction, matchingDescriptions);
      const stmts = dataTableActions(buildId, dataTable, factTableDef, notesCol, dataValuesCol, factIdentifiers);
      expect(stmts).toHaveLength(0);
    });
  });
});

// ===========================================================================
describe('setupLookupTableDimension — filter table sort direction', () => {
  const DATE_TYPES = [DimensionType.DatePeriod, DimensionType.Date];
  const NON_DATE_TYPES = [DimensionType.LookupTable];

  function callSetup(dimensionType: DimensionType) {
    const factTableCol = makeCol('date_col', 0, FactTableColumnType.Dimension);
    const dataset = makeDataset({ factTable: [factTableCol] });
    const dimension = makeDimension(dimensionType, 'date_col');
    const args = makeSetupLookupArgs();
    const stmts = setupLookupTableDimension(
      'b1',
      dataset,
      dimension,
      args.coreCubeViewSelectBuilder,
      args.columnNames,
      args.indexColumns,
      args.joinStatements,
      args.orderByStatements,
      args.viewConfig
    );
    return { stmts, orderByStatements: args.orderByStatements };
  }

  function filterInserts(stmts: string[]) {
    return stmts.filter((s) => s.includes('INSERT INTO') && s.includes('filter_table'));
  }

  for (const dimType of DATE_TYPES) {
    describe(`DimensionType.${dimType}`, () => {
      it('inserts into filter_table with ORDER BY sort_order DESC', () => {
        const { stmts } = callSetup(dimType);
        const inserts = filterInserts(stmts);
        expect(inserts.length).toBeGreaterThan(0);
        for (const stmt of inserts) {
          expect(stmt).toContain('ORDER BY sort_order DESC');
          expect(stmt).not.toContain('ORDER BY sort_order ASC');
        }
      });

      it('pushes DESC to orderByStatements for core view ORDER BY', () => {
        const { orderByStatements } = callSetup(dimType);
        expect(orderByStatements).toHaveLength(1);
        expect(orderByStatements[0]).toContain('sort_order DESC');
      });
    });
  }

  for (const dimType of NON_DATE_TYPES) {
    describe(`DimensionType.${dimType}`, () => {
      it('inserts into filter_table with ORDER BY sort_order ASC', () => {
        const { stmts } = callSetup(dimType);
        const inserts = filterInserts(stmts);
        expect(inserts.length).toBeGreaterThan(0);
        for (const stmt of inserts) {
          expect(stmt).toContain('ORDER BY sort_order ASC');
          expect(stmt).not.toContain('ORDER BY sort_order DESC');
        }
      });

      it('does not push DESC to orderByStatements for core view ORDER BY', () => {
        const { orderByStatements } = callSetup(dimType);
        expect(orderByStatements).toHaveLength(1);
        expect(orderByStatements[0]).not.toContain('DESC');
      });
    });
  }
});

// ===========================================================================
describe('updateFilterTableCounts', () => {
  const BUILD_ID = 'test-build-id';

  const COUNTED_TYPES = [
    FactTableColumnType.Dimension,
    FactTableColumnType.Time,
    FactTableColumnType.Measure,
    FactTableColumnType.NoteCodes,
    FactTableColumnType.Unknown,
    FactTableColumnType.LineNumber
  ] as const;

  const SKIPPED_TYPES = [FactTableColumnType.DataValues, FactTableColumnType.Ignore] as const;

  it('wraps statements in a transaction', () => {
    const { statements } = updateFilterTableCounts(BUILD_ID, []);
    expect(statements[0]).toBe('BEGIN TRANSACTION;');
    expect(statements[statements.length - 1]).toBe('COMMIT;');
  });

  it('returns buildStage UpdateFilterTable', () => {
    const block = updateFilterTableCounts(BUILD_ID, []);
    expect(block.buildStage).toBe(BuildStage.UpdateFilterTable);
  });

  it('always emits the metadata version UPDATE statement', () => {
    const { statements } = updateFilterTableCounts(BUILD_ID, []);
    const versionUpdate = statements.find(
      (s) => s.includes(METADATA_TABLE_NAME) && s.includes('SET value') && s.includes('filter_table_version')
    );
    expect(versionUpdate).toBeDefined();
    // FILTER_TABLE_VERSION is a number; pgformat %L renders it without quotes
    expect(versionUpdate).toContain(` ${FILTER_TABLE_VERSION} `);
  });

  it('always emits the metadata version INSERT-IF-NOT-EXISTS statement', () => {
    const { statements } = updateFilterTableCounts(BUILD_ID, []);
    const versionInsert = statements.find(
      (s) => s.includes(METADATA_TABLE_NAME) && s.includes('INSERT INTO') && s.includes('filter_table_version')
    );
    expect(versionInsert).toBeDefined();
    expect(versionInsert).toContain('WHERE NOT EXISTS');
    expect(versionInsert).toContain(` ${FILTER_TABLE_VERSION}`);
  });

  for (const colType of SKIPPED_TYPES) {
    it(`skips ${colType} columns — no UPDATE emitted for that column`, () => {
      const col = makeCol('skipped_col', 0, colType);
      const { statements } = updateFilterTableCounts(BUILD_ID, [col]);
      const updates = statements.filter((s) => s.startsWith('UPDATE') && s.includes('"skipped_col"'));
      expect(updates).toHaveLength(0);
    });
  }

  for (const colType of COUNTED_TYPES) {
    it(`emits a reference_count UPDATE for ${colType} columns`, () => {
      const col = makeCol('year_col', 0, colType);
      const { statements } = updateFilterTableCounts(BUILD_ID, [col]);
      const updates = statements.filter((s) => s.includes('reference_count'));
      expect(updates).toHaveLength(1);
    });
  }

  it('references the correct schema, fact table, and filter table in the UPDATE', () => {
    const col = makeCol('region', 0, FactTableColumnType.Dimension);
    const { statements } = updateFilterTableCounts(BUILD_ID, [col]);
    // pgformat only quotes identifiers that need it — simple names like filter_table are unquoted
    const update = statements.find((s) => s.includes('reference_count'))!;
    expect(update).toContain(`"${BUILD_ID}"`);
    expect(update).toContain(FILTER_TABLE_NAME);
    expect(update).toContain(FACT_TABLE_NAME);
    expect(update).toContain('region');
    expect(update).toContain('reference_count');
  });

  it('filters by fact_table_column using the column name literal', () => {
    const col = makeCol('region', 0, FactTableColumnType.Dimension);
    const { statements } = updateFilterTableCounts(BUILD_ID, [col]);
    const update = statements.find((s) => s.includes('reference_count'))!;
    // %L produces a quoted string literal — the column name must appear as a value, not an identifier
    expect(update).toContain("fact_table_column = 'region'");
  });

  it('emits one UPDATE per counted column', () => {
    const cols = [
      makeCol('region', 0, FactTableColumnType.Dimension),
      makeCol('year', 1, FactTableColumnType.Time),
      makeCol('value', 2, FactTableColumnType.DataValues), // skipped
      makeCol('notes', 3, FactTableColumnType.Ignore) // skipped
    ];
    const { statements } = updateFilterTableCounts(BUILD_ID, cols);
    const countUpdates = statements.filter((s) => s.includes('reference_count'));
    expect(countUpdates).toHaveLength(2);
  });

  it('produces no column UPDATE statements when all columns are skipped types', () => {
    const cols = [makeCol('value', 0, FactTableColumnType.DataValues), makeCol('flag', 1, FactTableColumnType.Ignore)];
    const { statements } = updateFilterTableCounts(BUILD_ID, cols);
    const countUpdates = statements.filter((s) => s.includes('reference_count'));
    expect(countUpdates).toHaveLength(0);
  });
});
