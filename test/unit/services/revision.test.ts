import { DataTableAction } from '../../../src/enums/data-table-action';
import { DimensionType } from '../../../src/enums/dimension-type';
import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { CubeBuildType } from '../../../src/enums/cube-build-type';
import { CubeBuildStatus } from '../../../src/enums/cube-build-status';
import { FactTableValidationException } from '../../../src/exceptions/fact-table-validation-exception';
import { CubeValidationException } from '../../../src/exceptions/cube-error-exception';
import { UnknownException } from '../../../src/exceptions/unknown.exception';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

// --- DB manager / query runner ---
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

// --- Repositories ---
const mockDatasetGetById = jest.fn();
jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: { getById: (...args: unknown[]) => mockDatasetGetById(...args) }
}));

const mockRevisionSave = jest.fn();
jest.mock('../../../src/repositories/revision', () => ({
  RevisionRepository: { save: (...args: unknown[]) => mockRevisionSave(...args) }
}));

// --- Entities with static methods ---
const mockStartBuild = jest.fn();
jest.mock('../../../src/entities/dataset/build-log', () => ({
  BuildLog: { startBuild: (...args: unknown[]) => mockStartBuild(...args) }
}));

const mockFactTableColumnFindOneOrFail = jest.fn();
jest.mock('../../../src/entities/dataset/fact-table-column', () => ({
  FactTableColumn: { findOneOrFail: (...args: unknown[]) => mockFactTableColumnFindOneOrFail(...args) }
}));

// --- cube-builder ---
const mockCreateAllCubeFiles = jest.fn();
const mockCreateLookupTableDimension = jest.fn();
const mockCreateMeasureLookupTable = jest.fn();
const mockUpdateFilterTableToLatest = jest.fn();
jest.mock('../../../src/services/cube-builder', () => ({
  createAllCubeFiles: (...args: unknown[]) => mockCreateAllCubeFiles(...args),
  createLookupTableDimension: (...args: unknown[]) => mockCreateLookupTableDimension(...args),
  createMeasureLookupTable: (...args: unknown[]) => mockCreateMeasureLookupTable(...args),
  makeCubeSafeString: (s: string) => s,
  updateFilterTableToLatest: (...args: unknown[]) => mockUpdateFilterTableToLatest(...args),
  FACT_TABLE_NAME: 'fact_table'
}));

// --- dimension-processor ---
const mockCreateDateDimensionLookup = jest.fn();
jest.mock('../../../src/services/dimension-processor', () => ({
  createDateDimensionLookup: (...args: unknown[]) => mockCreateDateDimensionLookup(...args)
}));

// --- lookup-table-utils ---
const mockBootstrapCubeBuildProcess = jest.fn();
const mockValidateHierarchy = jest.fn();
const mockValidateReference = jest.fn();
jest.mock('../../../src/utils/lookup-table-utils', () => ({
  bootstrapCubeBuildProcess: (...args: unknown[]) => mockBootstrapCubeBuildProcess(...args),
  validateLookupTableHierarchyValues: (...args: unknown[]) => mockValidateHierarchy(...args),
  validateLookupTableReferenceValues: (...args: unknown[]) => mockValidateReference(...args)
}));

// --- fact-table-validator ---
const mockFactTableValidatorFromSource = jest.fn();
const mockSourceAssignmentFromFactTable = jest.fn();
jest.mock('../../../src/services/fact-table-validator', () => ({
  factTableValidatorFromSource: (...args: unknown[]) => mockFactTableValidatorFromSource(...args),
  sourceAssignmentFromFactTable: (...args: unknown[]) => mockSourceAssignmentFromFactTable(...args)
}));

// --- revision utils ---
const mockRevisionStartAndEndDateFinder = jest.fn();
const mockWidenCoverageRange = jest.fn();
jest.mock('../../../src/utils/revision', () => ({
  revisionStartAndEndDateFinder: (...args: unknown[]) => mockRevisionStartAndEndDateFinder(...args),
  widenCoverageRange: (...args: unknown[]) => mockWidenCoverageRange(...args)
}));

// --- config ---
jest.mock('../../../src/config', () => ({
  config: { cube_builder: { preserve_failed: false } }
}));

// Import after mocks
import {
  attachUpdateDataTableToRevision,
  createDateTableInValidationCube,
  rebuildCubesForRevisions,
  rebuildAllFilterTablesForRevisions,
  updateRevisionTasks
} from '../../../src/services/revision';

// --- Helpers ---

function makeBuild(overrides: Record<string, unknown> = {}) {
  const build = {
    id: 'build-1',
    status: CubeBuildStatus.Completed,
    errors: null as string | null,
    completeBuild: jest.fn(),
    save: jest.fn(),
    reload: jest.fn(),
    ...overrides
  };
  build.save.mockResolvedValue(build);
  build.reload.mockResolvedValue(build);
  return build;
}

function makeDataset(overrides: Record<string, unknown> = {}) {
  return {
    id: 'ds-1',
    factTable: [{ columnName: 'col1', columnType: FactTableColumnType.Dimension }],
    measure: { id: 'measure-1', factTableColumn: 'measureCol', measureTable: [], metadata: [] },
    dimensions: [
      {
        id: 'dim-1',
        type: DimensionType.LookupTable,
        factTableColumn: 'col1',
        extractor: {},
        metadata: [],
        lookupTable: {},
        save: jest.fn()
      }
    ],
    revisions: [],
    ...overrides
  };
}

function makeDataTable() {
  return {
    action: undefined as DataTableAction | undefined,
    dataTableDescriptions: [{ columnName: 'col1', factTableColumn: undefined as string | undefined }],
    revision: undefined as unknown,
    save: jest.fn().mockResolvedValue(undefined),
    remove: jest.fn().mockResolvedValue(undefined)
  };
}

function makeRevision() {
  return {
    id: 'rev-1',
    startDate: new Date('2020-01-01'),
    endDate: new Date('2020-12-31'),
    dataTable: undefined as unknown,
    tasks: undefined as unknown,
    save: jest.fn().mockResolvedValue(undefined)
  };
}

describe('revision service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // happy-path defaults
    mockDatasetGetById.mockImplementation(async () => makeDataset());
    mockStartBuild.mockImplementation(async () => makeBuild());
    mockCreateAllCubeFiles.mockResolvedValue(undefined);
    mockSourceAssignmentFromFactTable.mockReturnValue({});
    mockFactTableValidatorFromSource.mockResolvedValue(undefined);
    mockFactTableColumnFindOneOrFail.mockResolvedValue({ columnName: 'measureCol' });
    mockCreateMeasureLookupTable.mockReturnValue(['SQL']);
    mockCreateLookupTableDimension.mockReturnValue('LOOKUP SQL');
    mockValidateReference.mockResolvedValue(false);
    mockValidateHierarchy.mockResolvedValue(false);
    mockRevisionStartAndEndDateFinder.mockReturnValue({ startDate: undefined, endDate: undefined });
    mockWidenCoverageRange.mockReturnValue({ startDate: new Date('2019-01-01'), endDate: new Date('2021-12-31') });
    mockBootstrapCubeBuildProcess.mockResolvedValue(undefined);
    mockUpdateFilterTableToLatest.mockResolvedValue(undefined);
  });

  describe('attachUpdateDataTableToRevision', () => {
    it('validates and attaches the data table on the happy path (auto column matching)', async () => {
      const revision = makeRevision();
      const dataTable = makeDataTable();

      await attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add);

      expect(dataTable.action).toBe(DataTableAction.Add);
      expect(revision.dataTable).toBe(dataTable);
      expect(mockCreateAllCubeFiles).toHaveBeenCalled();
      expect(mockFactTableValidatorFromSource).toHaveBeenCalled();
      expect(revision.save).toHaveBeenCalled();
      expect(dataTable.save).toHaveBeenCalled();
      // preserve_failed is false → validation cube cleaned up (DROP SCHEMA)
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining('DROP SCHEMA'));
    });

    it('matches columns explicitly when a column matcher is supplied', async () => {
      const revision = makeRevision();
      const dataTable = makeDataTable();
      const matcher = [{ fact_table_column_name: 'col1', data_table_column_name: 'col1' }];

      await attachUpdateDataTableToRevision(
        'ds-1',
        revision as never,
        dataTable as never,
        DataTableAction.Add,
        matcher as never
      );

      expect(dataTable.dataTableDescriptions[0].factTableColumn).toBe('col1');
      expect(mockCreateAllCubeFiles).toHaveBeenCalled();
    });

    it('throws UnknownException when the column matcher cannot match all fact table columns', async () => {
      const revision = makeRevision();
      const dataTable = makeDataTable();
      const matcher = [{ fact_table_column_name: 'nope', data_table_column_name: 'col1' }];

      await expect(
        attachUpdateDataTableToRevision(
          'ds-1',
          revision as never,
          dataTable as never,
          DataTableAction.Add,
          matcher as never
        )
      ).rejects.toThrow(UnknownException);
      expect(mockCreateAllCubeFiles).not.toHaveBeenCalled();
    });

    it('throws FactTableValidationException when auto matching leaves unmatched columns', async () => {
      mockDatasetGetById.mockResolvedValue(
        makeDataset({ factTable: [{ columnName: 'other', columnType: FactTableColumnType.Dimension }] })
      );
      const revision = makeRevision();
      const dataTable = makeDataTable();

      let caught: unknown;
      try {
        await attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add);
      } catch (e) {
        caught = e;
      }
      expect(caught).toBeInstanceOf(FactTableValidationException);
    });

    it('removes the data table and rethrows when the validation cube build fails', async () => {
      const error = new CubeValidationException('boom');
      mockCreateAllCubeFiles.mockRejectedValue(error);
      const revision = makeRevision();
      const dataTable = makeDataTable();

      await expect(
        attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add)
      ).rejects.toBe(error);
      expect(dataTable.remove).toHaveBeenCalled();
    });

    it('cleans up and rethrows when fact table validation fails', async () => {
      const error = new Error('fact table invalid');
      mockFactTableValidatorFromSource.mockRejectedValue(error);
      const revision = makeRevision();
      const dataTable = makeDataTable();

      await expect(
        attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add)
      ).rejects.toBe(error);
      expect(dataTable.remove).toHaveBeenCalled();
    });

    it('records a measure revision task when measure validation fails', async () => {
      mockValidateReference.mockResolvedValueOnce(true); // first call is for the measure
      const revision = makeRevision();
      const dataTable = makeDataTable();

      await attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add);

      expect((revision.tasks as { measure?: unknown }).measure).toEqual({ id: 'measure-1', lookupTableUpdated: false });
    });

    it('records a dimension revision task on non-matched lookup rows', async () => {
      // measure validation passes (false), dimension reference check returns true → DimensionNonMatchedRows
      mockValidateReference.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
      const revision = makeRevision();
      const dataTable = makeDataTable();

      await attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add);

      expect((revision.tasks as { dimensions: unknown[] }).dimensions).toEqual([
        { id: 'dim-1', lookupTableUpdated: false }
      ]);
    });

    it('throws BadRequestException when a dimension has no matching fact table column', async () => {
      mockDatasetGetById.mockResolvedValue(
        makeDataset({
          dimensions: [{ id: 'dim-1', type: DimensionType.LookupTable, factTableColumn: 'missing', save: jest.fn() }]
        })
      );
      const revision = makeRevision();
      const dataTable = makeDataTable();

      await expect(
        attachUpdateDataTableToRevision('ds-1', revision as never, dataTable as never, DataTableAction.Add)
      ).rejects.toThrow('errors.data_table_validation_error');
    });
  });

  describe('createDateTableInValidationCube', () => {
    it('updates the extractor coverage and saves the dimension', async () => {
      const savedDimension = { id: 'dim-1' };
      const dimension = {
        id: 'dim-1',
        extractor: {},
        lookupTable: undefined as unknown,
        save: jest.fn().mockResolvedValue(savedDimension)
      };
      mockCreateDateDimensionLookup.mockResolvedValue({
        startDate: new Date('2020-01-01'),
        endDate: new Date('2020-12-31'),
        lookupTable: { id: 'lt-1' }
      });

      const result = await createDateTableInValidationCube(
        'build-1',
        'ds-1',
        'col1_lookup',
        { columnName: 'col1' } as never,
        dimension as never
      );

      expect((dimension.extractor as { lookupTableStart?: Date }).lookupTableStart).toEqual(new Date('2020-01-01'));
      expect(dimension.lookupTable).toEqual({ id: 'lt-1' });
      expect(dimension.save).toHaveBeenCalled();
      expect(result).toBe(savedDimension);
    });
  });

  describe('rebuildCubesForRevisions', () => {
    function makeBuildLog(overrides: Record<string, unknown> = {}) {
      const log = {
        id: 'log-1',
        type: CubeBuildType.FullCube,
        buildScript: '',
        status: '' as CubeBuildStatus | string,
        errors: undefined as string | undefined,
        completeBuild: jest.fn(),
        save: jest.fn(),
        ...overrides
      };
      log.save.mockResolvedValue(log);
      return log;
    }

    const user = { id: 'user-1' } as never;

    it('marks the build log completed when every revision rebuilds successfully', async () => {
      const log = makeBuildLog();
      await rebuildCubesForRevisions(log as never, [{ id: 'rev-1', dataset_id: 'ds-1' } as never], user);

      expect(mockBootstrapCubeBuildProcess).toHaveBeenCalledWith('ds-1', 'rev-1');
      expect(mockCreateAllCubeFiles).toHaveBeenCalled();
      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Completed);
      expect(log.errors).toBeUndefined();
    });

    it('uses the draft cubes label when building draft cubes', async () => {
      const log = makeBuildLog({ type: CubeBuildType.DraftCubes });
      await rebuildCubesForRevisions(log as never, [{ id: 'rev-1', dataset_id: 'ds-1' } as never], user);
      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Completed);
    });

    it('records a failed build when bootstrapping throws', async () => {
      mockBootstrapCubeBuildProcess.mockRejectedValue(new Error('bootstrap failed'));
      const build = makeBuild();
      mockStartBuild.mockResolvedValue(build);
      const log = makeBuildLog();

      await rebuildCubesForRevisions(log as never, [{ id: 'rev-1', dataset_id: 'ds-1' } as never], user);

      expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
      expect(mockCreateAllCubeFiles).not.toHaveBeenCalled();
      expect(log.errors).toEqual(expect.stringContaining('bootstrap failed'));
    });

    it('records a failed build when building the cube files throws', async () => {
      mockCreateAllCubeFiles.mockRejectedValue(new Error('cube files failed'));
      const build = makeBuild();
      mockStartBuild.mockResolvedValue(build);
      const log = makeBuildLog();

      await rebuildCubesForRevisions(log as never, [{ id: 'rev-1', dataset_id: 'ds-1' } as never], user);

      expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
      expect(log.errors).toEqual(expect.stringContaining('cube files failed'));
    });

    it('records a failure when the reloaded build is not in the completed state', async () => {
      const build = makeBuild({ status: CubeBuildStatus.Failed, errors: 'kaboom' });
      mockStartBuild.mockResolvedValue(build);
      const log = makeBuildLog();

      await rebuildCubesForRevisions(log as never, [{ id: 'rev-1', dataset_id: 'ds-1' } as never], user);

      expect(log.errors).toEqual(expect.stringContaining('kaboom'));
      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Completed);
    });

    it('fails the whole build log when an unexpected error escapes the loop', async () => {
      mockStartBuild.mockRejectedValue(new Error('catastrophic'));
      const log = makeBuildLog();

      await rebuildCubesForRevisions(log as never, [{ id: 'rev-1', dataset_id: 'ds-1' } as never], user);

      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
    });
  });

  describe('rebuildAllFilterTablesForRevisions', () => {
    function makeBuildLog() {
      const log = {
        id: 'log-1',
        buildScript: '',
        status: '' as CubeBuildStatus | string,
        errors: undefined as string | undefined,
        completeBuild: jest.fn(),
        save: jest.fn()
      };
      log.save.mockResolvedValue(log);
      return log;
    }

    it('rebuilds filter tables for eligible revisions', async () => {
      const log = makeBuildLog();
      await rebuildAllFilterTablesForRevisions(log as never, [
        { id: 'rev-2', dataset_id: 'ds-1', data_table_id: 'dt-1', revision_index: 2 } as never
      ]);

      expect(mockUpdateFilterTableToLatest).toHaveBeenCalledWith('ds-1', 'rev-2');
      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Completed);
    });

    it('skips the first revision when it has no data table', async () => {
      const log = makeBuildLog();
      await rebuildAllFilterTablesForRevisions(log as never, [
        { id: 'rev-1', dataset_id: 'ds-1', data_table_id: null, revision_index: 1 } as never
      ]);

      expect(mockUpdateFilterTableToLatest).not.toHaveBeenCalled();
      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Completed);
    });

    it('records failures when rebuilding a filter table throws', async () => {
      mockUpdateFilterTableToLatest.mockRejectedValue(new Error('filter failed'));
      const log = makeBuildLog();

      await rebuildAllFilterTablesForRevisions(log as never, [
        { id: 'rev-2', dataset_id: 'ds-1', data_table_id: 'dt-1', revision_index: 2 } as never
      ]);

      expect(log.errors).toEqual(expect.stringContaining('filter failed'));
      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Completed);
    });

    it('fails the whole build log when an unexpected error escapes the loop', async () => {
      const log = makeBuildLog();
      // completeBuild(Completed) at the end of the happy path throws, dropping into the outer catch
      log.completeBuild.mockImplementationOnce(() => {
        throw new Error('completeBuild exploded');
      });

      await rebuildAllFilterTablesForRevisions(log as never, [
        { id: 'rev-2', dataset_id: 'ds-1', data_table_id: 'dt-1', revision_index: 2 } as never
      ]);

      expect(log.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
    });
  });

  describe('updateRevisionTasks', () => {
    it('does nothing for the first revision', async () => {
      const dataset = { draftRevision: { revisionIndex: 1, tasks: undefined } } as never;
      await updateRevisionTasks(dataset, 'dim-1', 'dimension');
      expect(mockRevisionSave).not.toHaveBeenCalled();
    });

    it('initialises dimension tasks when none exist', async () => {
      const revision = { revisionIndex: 2, tasks: undefined as unknown };
      const dataset = { draftRevision: revision } as never;

      await updateRevisionTasks(dataset, 'dim-1', 'dimension');

      expect(revision.tasks).toEqual({ dimensions: [{ id: 'dim-1', lookupTableUpdated: true }], measure: undefined });
      expect(mockRevisionSave).toHaveBeenCalledWith(revision);
    });

    it('initialises measure task when none exist', async () => {
      const revision = { revisionIndex: 2, tasks: undefined as unknown };
      const dataset = { draftRevision: revision } as never;

      await updateRevisionTasks(dataset, 'measure-1', 'measure');

      expect(revision.tasks).toEqual({ dimensions: [], measure: { id: 'measure-1', lookupTableUpdated: true } });
    });

    it('marks an existing dimension task as updated', async () => {
      const revision = {
        revisionIndex: 2,
        tasks: { dimensions: [{ id: 'dim-1', lookupTableUpdated: false }], measure: undefined }
      };
      const dataset = { draftRevision: revision } as never;

      await updateRevisionTasks(dataset, 'dim-1', 'dimension');

      expect(revision.tasks.dimensions[0].lookupTableUpdated).toBe(true);
    });

    it('appends a new dimension task when not already present', async () => {
      const revision = {
        revisionIndex: 2,
        tasks: { dimensions: [{ id: 'other', lookupTableUpdated: true }], measure: undefined }
      };
      const dataset = { draftRevision: revision } as never;

      await updateRevisionTasks(dataset, 'dim-1', 'dimension');

      expect(revision.tasks.dimensions).toHaveLength(2);
    });

    it('marks an existing measure task as updated', async () => {
      const revision = {
        revisionIndex: 2,
        tasks: { dimensions: [], measure: { id: 'measure-1', lookupTableUpdated: false } }
      };
      const dataset = { draftRevision: revision } as never;

      await updateRevisionTasks(dataset, 'measure-1', 'measure');

      expect(revision.tasks.measure.lookupTableUpdated).toBe(true);
    });

    it('sets the measure task when tasks exist but the measure is absent', async () => {
      const revision = { revisionIndex: 2, tasks: { dimensions: [], measure: undefined as unknown } };
      const dataset = { draftRevision: revision } as never;

      await updateRevisionTasks(dataset, 'measure-1', 'measure');

      expect(revision.tasks.measure).toEqual({ id: 'measure-1', lookupTableUpdated: true });
    });
  });
});
