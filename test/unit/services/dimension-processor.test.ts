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

jest.mock('../../../src/services/cube-builder', () => ({
  FACT_TABLE_NAME: 'fact_table',
  VALIDATION_TABLE_NAME: 'validation_table',
  makeCubeSafeString: (s: string) => s
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

const mockFileServiceDelete = jest.fn();
jest.mock('../../../src/utils/get-file-service', () => ({
  getFileService: () => ({
    delete: (...args: unknown[]) => mockFileServiceDelete(...args),
    saveBuffer: jest.fn()
  })
}));

// Active-record-style spies — these closures back the mocked Dimension /
// Measure / FactTableColumn / etc. and are referenced directly in tests.
const mockDimensionFindOneByOrFail = jest.fn();
const dimensionRepoDelete = jest.fn();
jest.mock('../../../src/entities/dataset/dimension', () => ({
  Dimension: {
    findOneByOrFail: (...args: unknown[]) => mockDimensionFindOneByOrFail(...args),
    create: jest.fn().mockImplementation((data: unknown) => ({
      ...(data as object),
      save: jest.fn().mockResolvedValue(undefined)
    })),
    getRepository: jest.fn().mockReturnValue({
      delete: (...args: unknown[]) => dimensionRepoDelete(...args)
    })
  }
}));

const mockLookupTableFindOneBy = jest.fn();
jest.mock('../../../src/entities/dataset/lookup-table', () => ({
  LookupTable: {
    findOneBy: (...args: unknown[]) => mockLookupTableFindOneBy(...args)
  }
}));

const measureRepoDelete = jest.fn();
jest.mock('../../../src/entities/dataset/measure', () => ({
  Measure: {
    create: jest.fn().mockImplementation((data: unknown) => ({
      ...(data as object),
      save: jest.fn().mockResolvedValue(undefined)
    })),
    getRepository: jest.fn().mockReturnValue({
      delete: (...args: unknown[]) => measureRepoDelete(...args)
    })
  }
}));

const measureRowRepoDelete = jest.fn();
jest.mock('../../../src/entities/dataset/measure-row', () => ({
  MeasureRow: {
    getRepository: jest.fn().mockReturnValue({
      delete: (...args: unknown[]) => measureRowRepoDelete(...args)
    })
  }
}));

const measureMetadataRepoDelete = jest.fn();
jest.mock('../../../src/entities/dataset/measure-metadata', () => ({
  MeasureMetadata: {
    create: jest.fn(),
    getRepository: jest.fn().mockReturnValue({
      delete: (...args: unknown[]) => measureMetadataRepoDelete(...args)
    })
  }
}));

jest.mock('../../../src/entities/dataset/dimension-metadata', () => ({
  DimensionMetadata: {
    create: jest.fn().mockImplementation((data: unknown) => data)
  }
}));

const mockFactTableColumnFindOneByOrFail = jest.fn();
const mockFactTableColumnFindBy = jest.fn();
const mockFactTableColumnSave = jest.fn();
const ftcRepoDelete = jest.fn();
jest.mock('../../../src/entities/dataset/fact-table-column', () => {
  // Construct a function (so callers can do `new FactTableColumn()`) with the
  // static repository methods attached.
  const Ctor = function (this: Record<string, unknown>) {
    /* fields populated by caller */
  } as unknown as Record<string, unknown> & (new () => unknown);
  Ctor.findOneByOrFail = (...args: unknown[]) => mockFactTableColumnFindOneByOrFail(...args);
  Ctor.findBy = (...args: unknown[]) => mockFactTableColumnFindBy(...args);
  Ctor.save = (...args: unknown[]) => mockFactTableColumnSave(...args);
  Ctor.getRepository = () => ({ delete: (...args: unknown[]) => ftcRepoDelete(...args) });
  return { FactTableColumn: Ctor };
});

jest.mock('../../../src/utils/preview-generator', () => ({
  previewGenerator: jest.fn().mockReturnValue({ status: 200, preview: 'ok' }),
  sampleSize: 5
}));

jest.mock('../../../src/utils/mock-cube-handler', () => ({
  createPostgresValidationSchema: jest.fn().mockResolvedValue(undefined),
  cleanUpPostgresValidationSchema: jest.fn().mockResolvedValue(undefined)
}));

// Import after mocks
import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { DimensionType } from '../../../src/enums/dimension-type';
import { NumberType } from '../../../src/extractors/number-extractor';
import { SourceAssignmentException } from '../../../src/exceptions/source-assignment.exception';
import { SourceAssignmentDTO } from '../../../src/dtos/source-assignment-dto';
import { DataTable } from '../../../src/entities/dataset/data-table';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Dimension } from '../../../src/entities/dataset/dimension';
import { Revision } from '../../../src/entities/dataset/revision';
import {
  cleanUpDimension,
  cleanupDimensionMeasureAndFactTable,
  createDimensionsFromSourceAssignment,
  getDimensionPreview,
  getFactTableColumnPreview,
  removeAllDimensions,
  removeMeasure,
  setupTextDimension,
  validateNumericDimension,
  validateSourceAssignment
} from '../../../src/services/dimension-processor';

// The repo-delete spies are declared at top scope above, alongside their
// jest.mock factories — referenced directly by name in the tests below.

// --- Helpers ---

function makeSourceAssignment(overrides: Partial<SourceAssignmentDTO>[] = []): SourceAssignmentDTO[] {
  const base: SourceAssignmentDTO[] = [
    { column_index: 0, column_name: 'period', column_type: FactTableColumnType.Time },
    { column_index: 1, column_name: 'data', column_type: FactTableColumnType.DataValues },
    { column_index: 2, column_name: 'measure', column_type: FactTableColumnType.Measure },
    { column_index: 3, column_name: 'notes', column_type: FactTableColumnType.NoteCodes }
  ];
  return [...base, ...(overrides as SourceAssignmentDTO[])];
}

function makeDataTable(columnNames = ['period', 'data', 'measure', 'notes']): DataTable {
  return {
    dataTableDescriptions: columnNames.map((name, index) => ({
      columnName: name,
      columnIndex: index,
      columnDatatype: 'VARCHAR'
    }))
  } as DataTable;
}

function makeDimension(overrides: Partial<Dimension> = {}): Dimension {
  return {
    id: 'dim-1',
    extractor: null,
    joinColumn: null,
    type: DimensionType.Raw,
    factTableColumn: 'period',
    lookupTable: null,
    dataset: { id: 'dataset-1' } as Dataset,
    save: jest.fn().mockResolvedValue(undefined),
    ...overrides
  } as unknown as Dimension;
}

// --- Tests ---

describe('validateSourceAssignment', () => {
  it('classifies columns into their expected buckets', () => {
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes', 'extra']);
    const sources = [
      ...makeSourceAssignment(),
      { column_index: 4, column_name: 'extra', column_type: FactTableColumnType.Ignore }
    ];

    const result = validateSourceAssignment(dataTable, sources);

    expect(result.dataValues?.column_name).toBe('data');
    expect(result.measure?.column_name).toBe('measure');
    expect(result.noteCodes?.column_name).toBe('notes');
    expect(result.dimensions).toHaveLength(1);
    expect(result.dimensions[0].column_name).toBe('period');
    expect(result.ignore).toHaveLength(1);
    expect(result.ignore[0].column_name).toBe('extra');
  });

  it('treats both Time and Dimension columns as dimensions', () => {
    const dataTable = makeDataTable(['period', 'area', 'data', 'measure', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      { column_index: 0, column_name: 'period', column_type: FactTableColumnType.Time },
      { column_index: 1, column_name: 'area', column_type: FactTableColumnType.Dimension },
      { column_index: 2, column_name: 'data', column_type: FactTableColumnType.DataValues },
      { column_index: 3, column_name: 'measure', column_type: FactTableColumnType.Measure },
      { column_index: 4, column_name: 'notes', column_type: FactTableColumnType.NoteCodes }
    ];

    const result = validateSourceAssignment(dataTable, sources);
    expect(result.dimensions).toHaveLength(2);
    expect(result.dimensions.map((d) => d.column_name)).toEqual(['period', 'area']);
  });

  it('handles a dataTable with no dataTableDescriptions by treating valid columns as empty', () => {
    // No descriptions → validColumnNames is [] → any source assignment is rejected
    const dataTable = { dataTableDescriptions: undefined } as unknown as DataTable;
    const sources = makeSourceAssignment();

    expect(() => validateSourceAssignment(dataTable, sources)).toThrow(SourceAssignmentException);
    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('invalid_column_name');
  });

  it('throws when a source references a column that does not exist in the data table', () => {
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      ...makeSourceAssignment(),
      { column_index: 4, column_name: 'ghost', column_type: FactTableColumnType.Dimension }
    ];

    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('invalid_column_name');
  });

  it('throws when more than one data values column is supplied', () => {
    const dataTable = makeDataTable(['period', 'data', 'data2', 'measure', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      ...makeSourceAssignment(),
      { column_index: 4, column_name: 'data2', column_type: FactTableColumnType.DataValues }
    ];

    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('too_many_data_values');
  });

  it('throws when more than one measure column is supplied', () => {
    const dataTable = makeDataTable(['period', 'data', 'measure', 'measure2', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      ...makeSourceAssignment(),
      { column_index: 4, column_name: 'measure2', column_type: FactTableColumnType.Measure }
    ];

    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('too_many_measure');
  });

  it('throws when more than one note codes column is supplied', () => {
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes', 'notes2']);
    const sources: SourceAssignmentDTO[] = [
      ...makeSourceAssignment(),
      { column_index: 4, column_name: 'notes2', column_type: FactTableColumnType.NoteCodes }
    ];

    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('too_many_footnotes');
  });

  it('throws when an unrecognised column type is supplied', () => {
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes']);
    const sources = [
      ...makeSourceAssignment(),
      // Coerce an invalid type that the switch statement won't recognise
      { column_index: 0, column_name: 'period', column_type: 'bogus' as unknown as FactTableColumnType }
    ];

    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('invalid_source_type');
  });

  it('throws when there is no data values column', () => {
    const dataTable = makeDataTable(['period', 'measure', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      { column_index: 0, column_name: 'period', column_type: FactTableColumnType.Time },
      { column_index: 1, column_name: 'measure', column_type: FactTableColumnType.Measure },
      { column_index: 2, column_name: 'notes', column_type: FactTableColumnType.NoteCodes }
    ];
    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('missing_data_values');
  });

  it('throws when there is no measure column', () => {
    const dataTable = makeDataTable(['period', 'data', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      { column_index: 0, column_name: 'period', column_type: FactTableColumnType.Time },
      { column_index: 1, column_name: 'data', column_type: FactTableColumnType.DataValues },
      { column_index: 2, column_name: 'notes', column_type: FactTableColumnType.NoteCodes }
    ];
    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('missing_measure');
  });

  it('throws when there is no note codes column', () => {
    const dataTable = makeDataTable(['period', 'data', 'measure']);
    const sources: SourceAssignmentDTO[] = [
      { column_index: 0, column_name: 'period', column_type: FactTableColumnType.Time },
      { column_index: 1, column_name: 'data', column_type: FactTableColumnType.DataValues },
      { column_index: 2, column_name: 'measure', column_type: FactTableColumnType.Measure }
    ];
    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('missing_footnotes');
  });

  it('throws when there are no dimensions', () => {
    const dataTable = makeDataTable(['data', 'measure', 'notes']);
    const sources: SourceAssignmentDTO[] = [
      { column_index: 0, column_name: 'data', column_type: FactTableColumnType.DataValues },
      { column_index: 1, column_name: 'measure', column_type: FactTableColumnType.Measure },
      { column_index: 2, column_name: 'notes', column_type: FactTableColumnType.NoteCodes }
    ];
    expect(() => validateSourceAssignment(dataTable, sources)).toThrow('missing_dimensions');
  });
});

describe('setupTextDimension', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('sets the dimension type to Text and gives it a text extractor', async () => {
    const dimension = makeDimension();
    const reloaded = {
      id: 'dim-1',
      type: DimensionType.Raw,
      extractor: null,
      save: jest.fn().mockResolvedValue(undefined)
    };
    mockDimensionFindOneByOrFail.mockResolvedValueOnce(reloaded);

    await setupTextDimension(dimension);

    expect(mockDimensionFindOneByOrFail).toHaveBeenCalledWith({ id: 'dim-1' });
    expect(reloaded.type).toBe(DimensionType.Text);
    expect(reloaded.extractor).toEqual({ type: 'text' });
    expect(reloaded.save).toHaveBeenCalled();
  });

  it('cleans up an existing extractor before assigning the new text one', async () => {
    const saveSpy = jest.fn().mockResolvedValue(undefined);
    const dimension = makeDimension({
      extractor: { type: 'number' } as unknown as Dimension['extractor'],
      save: saveSpy
    });
    const reloaded = {
      id: 'dim-1',
      save: jest.fn().mockResolvedValue(undefined)
    };
    mockDimensionFindOneByOrFail.mockResolvedValueOnce(reloaded);

    await setupTextDimension(dimension);

    // cleanUpDimension is internal — easiest signal that it ran is that the
    // passed dimension was saved once before the reload-and-update path ran.
    expect(saveSpy).toHaveBeenCalled();
    expect(reloaded.save).toHaveBeenCalled();
  });
});

describe('cleanUpDimension', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('resets fields on the dimension, saves it, and does nothing when there is no lookup table', async () => {
    const dimension = makeDimension({
      extractor: { type: 'text' } as unknown as Dimension['extractor'],
      joinColumn: 'code',
      type: DimensionType.Text,
      lookupTable: null
    });

    await cleanUpDimension(dimension);

    expect(dimension.extractor).toBeNull();
    expect(dimension.joinColumn).toBeNull();
    expect(dimension.type).toBe(DimensionType.Raw);
    expect(dimension.lookupTable).toBeNull();
    expect(dimension.save).toHaveBeenCalled();
    expect(mockLookupTableFindOneBy).not.toHaveBeenCalled();
    expect(mockFileServiceDelete).not.toHaveBeenCalled();
  });

  it('removes the previous lookup table entity and its file when one was attached', async () => {
    const remove = jest.fn().mockResolvedValue(undefined);
    mockLookupTableFindOneBy.mockResolvedValueOnce({ id: 'lt-1', remove });
    mockFileServiceDelete.mockResolvedValueOnce(undefined);

    const dimension = makeDimension({
      lookupTable: { id: 'lt-1', filename: 'lt.csv' } as unknown as Dimension['lookupTable']
    });

    await cleanUpDimension(dimension);

    expect(mockLookupTableFindOneBy).toHaveBeenCalledWith({ id: 'lt-1' });
    expect(remove).toHaveBeenCalled();
    expect(mockFileServiceDelete).toHaveBeenCalledWith('lt.csv', 'dataset-1');
  });

  it('rethrows when saving the dimension fails', async () => {
    const dimension = makeDimension({
      save: jest.fn().mockRejectedValueOnce(new Error('boom'))
    });

    await expect(cleanUpDimension(dimension)).rejects.toThrow('boom');
  });

  it('swallows file-service errors so the cleanup still completes', async () => {
    const remove = jest.fn().mockResolvedValue(undefined);
    mockLookupTableFindOneBy.mockResolvedValueOnce({ id: 'lt-1', remove });
    mockFileServiceDelete.mockRejectedValueOnce(new Error('blob 500'));

    const dimension = makeDimension({
      lookupTable: { id: 'lt-1', filename: 'lt.csv' } as unknown as Dimension['lookupTable']
    });

    // No throw despite the blob error
    await expect(cleanUpDimension(dimension)).resolves.toBeUndefined();
    expect(remove).toHaveBeenCalled();
  });

  it('handles the case where the lookup table was already removed from the database', async () => {
    mockLookupTableFindOneBy.mockResolvedValueOnce(null);
    mockFileServiceDelete.mockResolvedValueOnce(undefined);

    const dimension = makeDimension({
      lookupTable: { id: 'lt-1', filename: 'lt.csv' } as unknown as Dimension['lookupTable']
    });

    await cleanUpDimension(dimension);

    // Still tries the file removal because the id+filename were captured before the null lookup
    expect(mockFileServiceDelete).toHaveBeenCalledWith('lt.csv', 'dataset-1');
  });
});

describe('removeAllDimensions', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('deletes dimensions even when the dataset has none attached', async () => {
    const dataset = { id: 'd-1', dimensions: undefined } as unknown as Dataset;
    await removeAllDimensions(dataset);
    expect(dimensionRepoDelete).toHaveBeenCalledWith({ dataset });
    expect(mockFileServiceDelete).not.toHaveBeenCalled();
  });

  it('removes lookup-table files for every dimension that has one', async () => {
    const dataset = {
      id: 'd-1',
      dimensions: [
        { lookupTable: { filename: 'one.csv' } },
        { lookupTable: { filename: 'two.csv' } },
        { lookupTable: null }
      ]
    } as unknown as Dataset;

    mockFileServiceDelete.mockResolvedValue(undefined);

    await removeAllDimensions(dataset);

    expect(mockFileServiceDelete).toHaveBeenCalledTimes(2);
    expect(mockFileServiceDelete).toHaveBeenCalledWith('one.csv', 'd-1');
    expect(mockFileServiceDelete).toHaveBeenCalledWith('two.csv', 'd-1');
    expect(dimensionRepoDelete).toHaveBeenCalledWith({ dataset });
  });

  it('continues deleting dimensions even if file removal throws', async () => {
    const dataset = {
      id: 'd-1',
      dimensions: [{ lookupTable: { filename: 'broken.csv' } }]
    } as unknown as Dataset;

    mockFileServiceDelete.mockRejectedValueOnce(new Error('blob unreachable'));

    await expect(removeAllDimensions(dataset)).resolves.toBeUndefined();
    expect(dimensionRepoDelete).toHaveBeenCalledWith({ dataset });
  });
});

describe('removeMeasure', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('deletes the measure rows, metadata and measure itself when there is no measure attached', async () => {
    const dataset = { id: 'd-1', measure: undefined } as unknown as Dataset;
    await removeMeasure(dataset);
    expect(measureRepoDelete).toHaveBeenCalledWith({ dataset });
    expect(measureRowRepoDelete).not.toHaveBeenCalled();
    expect(measureMetadataRepoDelete).not.toHaveBeenCalled();
  });

  it('removes the lookup table file and clears rows/metadata when the measure has a lookup table', async () => {
    const measure = { id: 'm-1', lookupTable: { filename: 'meas.csv' } };
    const dataset = { id: 'd-1', measure } as unknown as Dataset;

    await removeMeasure(dataset);

    expect(mockFileServiceDelete).toHaveBeenCalledWith('meas.csv', 'd-1');
    expect(measureRowRepoDelete).toHaveBeenCalledWith({ measure });
    expect(measureMetadataRepoDelete).toHaveBeenCalledWith({ measure });
    expect(measureRepoDelete).toHaveBeenCalledWith({ dataset });
  });

  it('still deletes rows, metadata and measure when there is a measure but no lookup table', async () => {
    const measure = { id: 'm-1', lookupTable: null };
    const dataset = { id: 'd-1', measure } as unknown as Dataset;

    await removeMeasure(dataset);

    expect(mockFileServiceDelete).not.toHaveBeenCalled();
    expect(measureRowRepoDelete).toHaveBeenCalledWith({ measure });
    expect(measureMetadataRepoDelete).toHaveBeenCalledWith({ measure });
    expect(measureRepoDelete).toHaveBeenCalledWith({ dataset });
  });
});

describe('cleanupDimensionMeasureAndFactTable', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('clears fact-table columns, dimensions and measure in turn', async () => {
    const dataset = {
      id: 'd-1',
      dimensions: [],
      measure: undefined
    } as unknown as Dataset;

    await cleanupDimensionMeasureAndFactTable(dataset);

    expect(ftcRepoDelete).toHaveBeenCalledWith({ id: 'd-1' });
    expect(dimensionRepoDelete).toHaveBeenCalledWith({ dataset });
    expect(measureRepoDelete).toHaveBeenCalledWith({ dataset });
  });
});

describe('validateNumericDimension', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // clearAllMocks doesn't purge the `mockResolvedValueOnce` queue — reset it
    // explicitly so each test starts with no leftover scripted responses.
    mockQuery.mockReset();
    mockRelease.mockReset();
  });

  function makeNumericDataset(): Dataset {
    return {
      id: 'dataset-1',
      draftRevision: { id: 'rev-1' } as Revision
    } as Dataset;
  }

  function makeNumericDimension(): Dimension {
    const dimension = {
      id: 'dim-1',
      factTableColumn: 'value',
      extractor: null,
      lookupTable: null,
      joinColumn: null,
      type: DimensionType.Raw
    } as unknown as Dimension;
    // Mirror TypeORM BaseEntity.save: returns `this` with whatever the caller
    // just assigned to the entity. Callers in dimension-processor.ts assign
    // `extractor` (and others) before awaiting `save()`.
    (dimension as unknown as { save: jest.Mock }).save = jest.fn(async () => dimension);
    return dimension;
  }

  it('throws when no number type is supplied', async () => {
    await expect(
      validateNumericDimension(
        { number_format: undefined } as Parameters<typeof validateNumericDimension>[0],
        makeNumericDataset(),
        makeNumericDimension()
      )
    ).rejects.toThrow('No number type supplied');
  });

  it('throws when number type is Decimal but no decimal_places is supplied', async () => {
    await expect(
      validateNumericDimension(
        { number_format: NumberType.Decimal, decimal_places: undefined } as Parameters<
          typeof validateNumericDimension
        >[0],
        makeNumericDataset(),
        makeNumericDimension()
      )
    ).rejects.toThrow('No decimal places supplied');
  });

  it('returns an error view when the schema-info query fails', async () => {
    mockQuery.mockRejectedValueOnce(new Error('schema gone'));

    const result = await validateNumericDimension(
      { number_format: NumberType.Integer } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toMatchObject({ status: 400 });
    expect(mockRelease).toHaveBeenCalled();
  });

  it('fast-paths to a preview when the column is already a native integer type', async () => {
    mockQuery
      // schema-info: column is BIGINT
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'BIGINT' }])
      // totals query
      .mockResolvedValueOnce([{ totalLines: 10 }])
      // preview query
      .mockResolvedValueOnce([{ value: 1 }, { value: 2 }]);

    const result = await validateNumericDimension(
      { number_format: NumberType.Integer } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('fast-paths to a preview when the column is already a native float type and the request is Decimal', async () => {
    mockQuery
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'DOUBLE' }])
      .mockResolvedValueOnce([{ totalLines: 4 }])
      .mockResolvedValueOnce([{ value: 1.1 }]);

    const result = await validateNumericDimension(
      { number_format: NumberType.Decimal, decimal_places: 2 } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('returns a non-numerical-values error when text data does not match the requested integer format', async () => {
    mockQuery
      // schema-info: column is VARCHAR (no fast path)
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'VARCHAR' }])
      // non-matching query: rows present
      .mockResolvedValueOnce([{ value: 'abc' }, { value: 'N/A' }])
      // distinct non-matching values
      .mockResolvedValueOnce([{ value: 'abc' }, { value: 'N/A' }]);

    const result = await validateNumericDimension(
      { number_format: NumberType.Integer } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toMatchObject({ status: 400 });
  });

  it('returns 500 when the non-matching query itself blows up', async () => {
    mockQuery
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'VARCHAR' }])
      .mockRejectedValueOnce(new Error('db lost'));

    const result = await validateNumericDimension(
      { number_format: NumberType.Integer } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toMatchObject({ status: 500 });
  });

  it('returns 400 when the distinct-non-matching query fails after non-matching rows were found', async () => {
    mockQuery
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'VARCHAR' }])
      .mockResolvedValueOnce([{ value: 'abc' }])
      .mockRejectedValueOnce(new Error('distinct query failed'));

    const result = await validateNumericDimension(
      { number_format: NumberType.Integer } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toMatchObject({ status: 400 });
  });

  it('validates and previews when text data does match the requested integer format', async () => {
    mockQuery
      // schema info: VARCHAR — no fast path
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'VARCHAR' }])
      // non-matching query: empty (everything matches)
      .mockResolvedValueOnce([])
      // totals query
      .mockResolvedValueOnce([{ totalLines: 5 }])
      // preview query
      .mockResolvedValueOnce([{ value: 1 }, { value: 2 }]);

    const result = await validateNumericDimension(
      { number_format: NumberType.Integer } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('validates and previews decimals when text data matches the requested decimal format', async () => {
    mockQuery
      .mockResolvedValueOnce([{ column_name: 'value', data_type: 'VARCHAR' }])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ totalLines: 5 }])
      .mockResolvedValueOnce([{ value: 1.23 }]);

    const result = await validateNumericDimension(
      { number_format: NumberType.Decimal, decimal_places: 2 } as Parameters<typeof validateNumericDimension>[0],
      makeNumericDataset(),
      makeNumericDimension()
    );

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });
});

describe('getDimensionPreview', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockQuery.mockReset();
    mockRelease.mockReset();
  });

  function makePreviewDataset(): Dataset {
    return {
      id: 'dataset-1',
      draftRevision: { id: 'rev-1' } as Revision
    } as Dataset;
  }

  it('routes Date dimensions through the date preview helper', async () => {
    // 1: totals; 2: date preview query
    mockQuery
      .mockResolvedValueOnce([{ totalLines: 12 }])
      .mockResolvedValueOnce([{ reference: '202324', description: 'Apr 2023 - Mar 2024' }]);

    const dimension = makeDimension({ type: DimensionType.Date });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('routes DatePeriod dimensions through the same date preview helper', async () => {
    mockQuery
      .mockResolvedValueOnce([{ totalLines: 4 }])
      .mockResolvedValueOnce([{ reference: '2023', description: '' }]);

    const dimension = makeDimension({ type: DimensionType.DatePeriod });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('returns a 500 error view when the date preview query fails', async () => {
    mockQuery.mockResolvedValueOnce([{ totalLines: 4 }]).mockRejectedValueOnce(new Error('lookup table missing'));

    const dimension = makeDimension({ type: DimensionType.Date });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toMatchObject({ status: 500 });
  });

  it('routes LookupTable dimensions through the lookup preview helper', async () => {
    // 1: totals; 2: tableDetails (column names); 3: actual preview
    mockQuery
      .mockResolvedValueOnce([{ totalLines: 3 }])
      .mockResolvedValueOnce([{ column_name: 'period' }, { column_name: 'sort_order' }, { column_name: 'language' }])
      .mockResolvedValueOnce([{ period: 'A', sort_order: 1 }]);

    const dimension = makeDimension({ type: DimensionType.LookupTable });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('returns 500 when the lookup tableDetails query fails', async () => {
    mockQuery.mockResolvedValueOnce([{ totalLines: 3 }]).mockRejectedValueOnce(new Error('schema unavailable'));

    const dimension = makeDimension({ type: DimensionType.LookupTable });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toMatchObject({ status: 500 });
  });

  it('returns 500 when the lookup preview query itself fails', async () => {
    mockQuery
      .mockResolvedValueOnce([{ totalLines: 3 }])
      .mockResolvedValueOnce([{ column_name: 'period' }])
      .mockRejectedValueOnce(new Error('preview query failed'));

    const dimension = makeDimension({ type: DimensionType.LookupTable });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toMatchObject({ status: 500 });
  });

  it('routes Text dimensions through the no-extractor preview helper', async () => {
    mockQuery.mockResolvedValueOnce([{ totalLines: 7 }]).mockResolvedValueOnce([{ period: 'A' }, { period: 'B' }]);

    const dimension = makeDimension({ type: DimensionType.Text });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('returns 500 when the text-dimension preview query fails', async () => {
    mockQuery.mockResolvedValueOnce([{ totalLines: 7 }]).mockRejectedValueOnce(new Error('table missing'));

    const dimension = makeDimension({ type: DimensionType.Text });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toMatchObject({ status: 500 });
  });

  it('routes Numeric (integer) dimensions through the number-extractor preview helper', async () => {
    mockQuery.mockResolvedValueOnce([{ totalLines: 9 }]).mockResolvedValueOnce([{ value: 1 }, { value: 2 }]);

    const dimension = makeDimension({
      type: DimensionType.Numeric,
      factTableColumn: 'value',
      extractor: { type: NumberType.Integer, decimalPlaces: 0 } as unknown as Dimension['extractor']
    });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('falls back to the no-extractor preview helper for unknown dimension types', async () => {
    mockQuery.mockResolvedValueOnce([{ totalLines: 2 }]).mockResolvedValueOnce([{ period: 'A' }]);

    const dimension = makeDimension({ type: 'mystery' as unknown as DimensionType });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });

  it('returns totalLines: -1 when the totals query fails, and still produces a preview', async () => {
    // Totals failure is caught and returns -1; the type dispatch continues.
    mockQuery.mockRejectedValueOnce(new Error('count failed')).mockResolvedValueOnce([{ period: 'A' }]);

    const dimension = makeDimension({ type: DimensionType.Text });
    const result = await getDimensionPreview(makePreviewDataset(), dimension, 'en-GB');

    expect(result).toEqual({ status: 200, preview: 'ok' });
  });
});

describe('getFactTableColumnPreview', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockQuery.mockReset();
    mockRelease.mockReset();
  });

  it('returns a successful preview when the query returns rows', async () => {
    mockQuery.mockResolvedValueOnce([{ region: 'Wales' }, { region: 'Cymru' }]);

    const result = await getFactTableColumnPreview({ id: 'ds-1' } as Dataset, 'rev-1', 'region');

    expect(result).toEqual({ status: 200, preview: 'ok' });
    expect(mockRelease).toHaveBeenCalled();
  });

  it('returns a 500 error view when the query fails', async () => {
    mockQuery.mockRejectedValueOnce(new Error('column missing'));

    const result = await getFactTableColumnPreview({ id: 'ds-1' } as Dataset, 'rev-1', 'region');

    expect(result).toMatchObject({ status: 500 });
    expect(mockRelease).toHaveBeenCalled();
  });
});

describe('createDimensionsFromSourceAssignment', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockQuery.mockReset();
    mockRelease.mockReset();
    mockFactTableColumnFindOneByOrFail.mockReset();
    mockFactTableColumnFindBy.mockReset();
    mockFactTableColumnSave.mockReset();
  });

  function makeColumn(name: string) {
    return {
      columnName: name,
      columnType: FactTableColumnType.Unknown,
      columnDatatype: 'VARCHAR',
      save: jest.fn().mockResolvedValue(undefined)
    };
  }

  it('processes a full source assignment: data values, measure, note codes and dimensions', async () => {
    const dataset = { id: 'd-1', dimensions: [], measure: undefined } as unknown as Dataset;
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes']);

    const dataColumn = makeColumn('data');
    const measureColumn = makeColumn('measure');
    const notesColumn = makeColumn('notes');
    const periodColumn = makeColumn('period');

    mockFactTableColumnFindOneByOrFail
      // updateDataValueColumn
      .mockResolvedValueOnce(dataColumn)
      // createUpdateNoteCodes
      .mockResolvedValueOnce(notesColumn)
      // createUpdateMeasure
      .mockResolvedValueOnce(measureColumn)
      // createUpdateDimension
      .mockResolvedValueOnce(periodColumn);

    // removeIgnoreAndUnknownColumns runs three findBy calls: all-columns, then
    // Unknown-columns twice. With no ignore columns the loop body is skipped.
    mockFactTableColumnFindBy.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([]);

    const validated = validateSourceAssignment(dataTable, makeSourceAssignment());
    await createDimensionsFromSourceAssignment(dataset, dataTable, validated);

    expect(dataColumn.columnType).toBe(FactTableColumnType.DataValues);
    expect(measureColumn.columnType).toBe(FactTableColumnType.Measure);
    expect(notesColumn.columnType).toBe(FactTableColumnType.NoteCodes);
    expect(dataColumn.save).toHaveBeenCalled();
    expect(measureColumn.save).toHaveBeenCalled();
    expect(notesColumn.save).toHaveBeenCalled();
    // Initial cleanup deletes existing dimension/measure/fact table rows
    expect(ftcRepoDelete).toHaveBeenCalledWith({ id: 'd-1' });
    expect(dimensionRepoDelete).toHaveBeenCalledWith({ dataset });
  });

  it('marks an ignore column with the Ignore column type', async () => {
    const dataset = { id: 'd-1', dimensions: [], measure: undefined } as unknown as Dataset;
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes', 'extra']);

    const dataColumn = makeColumn('data');
    const notesColumn = makeColumn('notes');
    const measureColumn = makeColumn('measure');
    const periodColumn = makeColumn('period');
    const extraColumn = makeColumn('extra');

    mockFactTableColumnFindOneByOrFail
      .mockResolvedValueOnce(dataColumn)
      .mockResolvedValueOnce(notesColumn)
      .mockResolvedValueOnce(measureColumn)
      .mockResolvedValueOnce(periodColumn);

    // findBy returns the full column list for the ignore matching loop, then
    // empty for the "any unknown columns left?" check.
    mockFactTableColumnFindBy.mockResolvedValueOnce([extraColumn]).mockResolvedValueOnce([]).mockResolvedValueOnce([]);

    const sources = [
      ...makeSourceAssignment(),
      { column_index: 4, column_name: 'extra', column_type: FactTableColumnType.Ignore }
    ];
    const validated = validateSourceAssignment(dataTable, sources);

    await createDimensionsFromSourceAssignment(dataset, dataTable, validated);

    expect(extraColumn.columnType).toBe(FactTableColumnType.Ignore);
    expect(extraColumn.save).toHaveBeenCalled();
  });

  it('rethrows when unknown columns remain after ignore processing', async () => {
    const dataset = { id: 'd-1', dimensions: [], measure: undefined } as unknown as Dataset;
    const dataTable = makeDataTable(['period', 'data', 'measure', 'notes']);

    mockFactTableColumnFindOneByOrFail
      .mockResolvedValueOnce(makeColumn('data'))
      .mockResolvedValueOnce(makeColumn('notes'))
      .mockResolvedValueOnce(makeColumn('measure'))
      .mockResolvedValueOnce(makeColumn('period'));

    // First findBy: full list. Second: unknown-column check returns one
    // unprocessed column → triggers SourceAssignmentException
    mockFactTableColumnFindBy
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ columnName: 'orphan', columnDatatype: FactTableColumnType.Unknown }])
      .mockResolvedValueOnce([{ columnName: 'orphan', columnDatatype: FactTableColumnType.Unknown }]);

    const validated = validateSourceAssignment(dataTable, makeSourceAssignment());
    await expect(createDimensionsFromSourceAssignment(dataset, dataTable, validated)).rejects.toThrow(
      SourceAssignmentException
    );
  });
});
