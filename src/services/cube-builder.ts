import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Revision } from '../entities/dataset/revision';
import { logger } from '../utils/logger';
import { performance } from 'node:perf_hooks';
import { Locale } from '../enums/locale';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { CubeValidationType } from '../enums/cube-validation-type';
import { performanceReporting } from '../utils/performance-reporting';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { dbManager } from '../db/database-manager';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableAction } from '../enums/data-table-action';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { MeasureRow } from '../entities/dataset/measure-row';
import { DimensionType } from '../enums/dimension-type';
import { Dimension } from '../entities/dataset/dimension';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { FindOptionsRelations } from 'typeorm';
import { DatasetRepository } from '../repositories/dataset';
import cubeConfig from '../config/cube-view.json';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';
import { config } from '../config';
import { CubeViewBuilder } from '../interfaces/cube-view-builder';
import { BuildLog } from '../entities/dataset/build-log';
import { CubeBuilder } from '../interfaces/cube-builder';
import { CubeMetaDataKeys } from '../enums/cube-metadata-keys';
import { BuildStage } from '../enums/build-stage';
import { FactTableInfo } from '../interfaces/fact-table-info';
import { TransactionBlock } from '../interfaces/transaction-block';
import { NoteCode, NoteCodes } from '../enums/note-code';
import { UniqueMeasureDetails } from '../interfaces/unique-measure-details';
import { MeasureFormat } from '../interfaces/measure-format';
import { RevisionRepository } from '../repositories/revision';

export const FACT_TABLE_NAME = 'fact_table';
export const METADATA_TABLE_NAME = 'metadata';
export const FILTER_TABLE_NAME = 'filter_table';
export const VALIDATION_TABLE_NAME = 'validation_table';
export const CORE_VIEW_NAME = 'core_view';

// Create the cube in the postgres database.  Handles the following:
// - Getting the full dataset for the build
// - bootstrapping the build process by making sure all lookup tables are present
// - Triggers the cube build process
// - Triggers materialization of the core view in the cube
export const createAllCubeFiles = async (
  datasetId: string,
  buildRevisionId: string,
  userId?: string,
  buildType = CubeBuildType.FullCube,
  buildId = crypto.randomUUID()
): Promise<void> => {
  // const datasetRelations: FindOptionsRelations<Dataset> = {
  //   factTable: true,
  //   dimensions: { metadata: true, lookupTable: true },
  //   measure: { metadata: true, measureTable: true },
  //   revisions: { dataTable: { dataTableDescriptions: true } }
  // };

  const datasetRelations: FindOptionsRelations<Dataset> = {
    factTable: true,
    dimensions: { metadata: true, lookupTable: true },
    measure: { metadata: true, measureTable: true }
  };

  logger.debug('Loading dataset and relations');
  const dataset = await DatasetRepository.getById(datasetId, datasetRelations);
  logger.debug('Loading revision and relations');
  const buildRevision = await RevisionRepository.getById(buildRevisionId, {
    dataTable: { dataTableDescriptions: true }
  });

  if (!buildRevision) {
    logger.error('Unable to find buildRevision in dataset.');
    throw new CubeValidationException('Failed to find buildRevision in dataset.');
  }

  const build = await BuildLog.startBuild(buildRevision, buildType, userId, buildId);

  if (!buildRevision.dataTable && buildRevision.revisionIndex === 1) {
    logger.warn(`First revision for revision ${buildRevision.id} has no data table.  Unable to proceed with build.`);
    build.completeBuild(CubeBuildStatus.Failed, undefined, '{ "message": "No data table in first revision."}');
    await build.save();
    throw new CubeValidationException('First revision has no data table.');
  }

  const cubeBuildConfig = cubeConfig.map((config) => {
    const columns = new Map<Locale, Set<string>>();
    const viewParts = new Map<Locale, string[]>();
    SUPPORTED_LOCALES.forEach((locale) => {
      columns.set(locale, new Set<string>());
      viewParts.set(locale, []);
    });
    return {
      name: config.name,
      config: config,
      columns,
      viewParts
    } as CubeViewBuilder;
  });
  logger.debug(`Build type = ${buildType}`);

  const createBuildSchemaRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    logger.info(`Creating schema for cube ${build.id}`);
    await createBuildSchemaRunner.query(pgformat(`CREATE SCHEMA IF NOT EXISTS %I;`, build.id));
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create the cube schema');
    const buildErr = {
      message: 'Something went wrong trying to create the cube schema',
      error
    };
    build.completeBuild(CubeBuildStatus.Failed, undefined, JSON.stringify(buildErr));
    await build.save();
    throw error;
  } finally {
    void createBuildSchemaRunner.release();
  }

  let cubeBuild: CubeBuilder;
  try {
    cubeBuild = await createBasePostgresCube(build, dataset, buildRevision, buildType, cubeBuildConfig);
  } catch (err) {
    logger.error(err, 'createAllCubeFiles: Something went wrong during the actual build process');
    if (config.cube_builder.preserve_failed) {
      throw err;
    }
    const unwindBuildRunner = dbManager.getCubeDataSource().createQueryRunner();
    try {
      await unwindBuildRunner.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE', build.id));
    } catch (error) {
      logger.error(error, 'Failed to remove build schema from the data');
    } finally {
      void unwindBuildRunner.release();
    }
    throw err;
  }

  if (buildType === CubeBuildType.ValidationCube) {
    build.completeBuild(CubeBuildStatus.Completed);
    logger.debug('Validation cube build complete');
    return;
  }

  build.status = CubeBuildStatus.SchemaRename;
  await build.save();
  const createRenameSchemaRunner = dbManager.getCubeDataSource().createQueryRunner();
  const renameStatements = [
    'BEGIN TRANSACTION;',
    pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', buildRevision.id),
    pgformat('ALTER SCHEMA %I RENAME TO %I;', build.id, buildRevision.id),
    'COMMIT;'
  ];
  try {
    logger.debug(`Renaming ${build.id} to cube rev ${buildRevision.id}`);
    await createRenameSchemaRunner.query(renameStatements.join('\n'));
  } catch (err) {
    await createRenameSchemaRunner.query('ROLLBACK;');
    build.completeBuild(
      CubeBuildStatus.Failed,
      [build.buildScript, ...renameStatements].join('\n'),
      JSON.stringify(err)
    );
    await build.save();
    logger.error(err, 'Failed to create cube in Postgres');
    throw err;
  } finally {
    void createRenameSchemaRunner.release();
  }

  build.status = CubeBuildStatus.Materializing;
  await build.save();
  // don't wait for this, can happen in the background so we can send the response earlier
  logger.debug('Running async process...');
  void createMaterialisedView(buildRevisionId, dataset, build.id, cubeBuild, cubeBuildConfig).catch((err) => {
    logger.error(
      err,
      `[build ID: ${build.id}] An error occurred trying to create materialised view for revision ${buildRevisionId}`
    );
  });
};

// This is the core cube builder
async function createBasePostgresCube(
  build: BuildLog,
  dataset: Dataset,
  buildRevision: Revision,
  buildType: CubeBuildType,
  viewConfig: CubeViewBuilder[]
): Promise<CubeBuilder> {
  logger.debug(`Starting build ${build.id} and Creating base cube for revision ${buildRevision.id}`);
  const functionStart = performance.now();
  const coreCubeViewSelectBuilder = new Map<Locale, string[]>();
  const columnNames = new Map<Locale, Set<string>>();

  SUPPORTED_LOCALES.map((locale) => {
    coreCubeViewSelectBuilder.set(locale, []);
    columnNames.set(locale, new Set<string>());
  });

  const cubeBuilder: CubeBuilder = {
    buildStatus: CubeBuildStatus.Queued,
    transactionBlocks: [],
    coreViewSQL: new Map<Locale, string>()
  };

  const buildStart = performance.now();
  const factTableInfo = await setupCubeBuilder(dataset, build.id);
  cubeBuilder.transactionBlocks.push(
    createCubeBaseTables(buildRevision.id, build.id, factTableInfo.factTableCreationQuery)
  );
  performanceReporting(Math.round(performance.now() - functionStart), 1000, 'Base table creation');

  if (!factTableInfo.dataValuesColumn && !factTableInfo.notesCodeColumn) {
    buildType = CubeBuildType.BaseCube;
    build.type = CubeBuildType.BaseCube;
    await build.save();
  }

  if (buildType === CubeBuildType.FullCube) {
    const { transactionBlocks, coreViewSQLMap } = createFullCube(
      build.id,
      dataset,
      buildRevision,
      factTableInfo,
      coreCubeViewSelectBuilder,
      viewConfig
    );
    cubeBuilder.transactionBlocks.push(...transactionBlocks);
    cubeBuilder.coreViewSQL = coreViewSQLMap;
  } else if (buildType === CubeBuildType.ValidationCube) {
    const { transactionBlocks, coreViewSQLMap } = createValidationCube(
      build.id,
      dataset,
      buildRevision,
      factTableInfo,
      coreCubeViewSelectBuilder
    );
    cubeBuilder.transactionBlocks.push(...transactionBlocks);
    cubeBuilder.coreViewSQL = coreViewSQLMap;
  } else if (buildType === CubeBuildType.BaseCube) {
    const { transactionBlocks, coreViewSQL } = createCubeNoSources(
      build.id,
      buildRevision,
      factTableInfo,
      coreCubeViewSelectBuilder
    );
    cubeBuilder.transactionBlocks.push(...transactionBlocks);
    cubeBuilder.coreViewSQL = coreViewSQL;
  } else {
    throw new Error(`Unknown cube build type ${buildType}`);
  }

  const attemptedBuildScript: string[] = [];
  const fullBuildScript = cubeBuilder.transactionBlocks.map((blk) => blk.statements.join('\n'));
  const metaDataStatements = [
    'BEGIN TRANSACTION;',
    pgformat(
      'INSERT INTO %I.metadata VALUES (%L, %L);',
      build.id,
      CubeMetaDataKeys.BuildScript,
      fullBuildScript.join('\n')
    ),
    pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', build.id, CubeMetaDataKeys.BuildResults, 'SUCCESS'),
    'COMMIT;'
  ];
  cubeBuilder.transactionBlocks.push({ buildStage: BuildStage.PostBuildMetadata, statements: metaDataStatements });

  logger.debug(`Beginning database cube build with id ${build.id} for revision ${buildRevision.id}`);
  cubeBuilder.buildStatus = CubeBuildStatus.Building;
  const updateBuild = await BuildLog.findOneByOrFail({ id: build.id });
  updateBuild.status = CubeBuildStatus.Building;
  await updateBuild.save();
  for (const block of cubeBuilder.transactionBlocks) {
    logger.debug(`Building ${block.buildStage}`);
    attemptedBuildScript.push(...block.statements);
    const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
    try {
      logger.trace(`Running query:\n\n${block.statements.join('\n')}\n\n`);
      await cubeDB.query(block.statements.join('\n'));
    } catch (err) {
      await cubeDB.query('ROLLBACK;');
      build.completeBuild(CubeBuildStatus.Failed, attemptedBuildScript.join('\n'), JSON.stringify(err));
      await build.save();
      if (block.buildStage === BuildStage.BaseTables) {
        logger.fatal(err, `Unable to create base tables for build ${build.id}, has the database failed?`);
      } else {
        logger.error(err, 'Something went wrong trying to build the cube');
        if (!config.cube_builder.preserve_failed) {
          throw err;
        }
        const metaDataUpdateStatements = [
          'BEGIN TRANSACTION;',
          pgformat(
            'INSERT INTO %I.metadata VALUES (%L, %L);',
            build.id,
            CubeMetaDataKeys.BuildScript,
            attemptedBuildScript.join('\n')
          ),
          pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', build.id, CubeMetaDataKeys.BuildResults, err),
          'COMMIT;'
        ];
        await cubeDB.query(metaDataUpdateStatements.join('\n'));
      }
      throw err;
    } finally {
      void cubeDB.release();
    }
  }

  build.status = CubeBuildStatus.Materializing;
  build.buildScript = fullBuildScript.join('\n');
  await build.save();

  const end = performance.now();
  const functionTime = Math.round(end - functionStart);
  const buildTime = Math.round(end - buildStart);
  performanceReporting(buildTime, 5000, 'Cube build process');
  performanceReporting(functionTime, 5000, 'Cube build function in total');
  return cubeBuilder;
}

async function createMaterialisedView(
  revisionId: string,
  dataset: Dataset,
  buildId: string,
  cubeBuilder: CubeBuilder,
  viewConfig: CubeViewBuilder[]
): Promise<void> {
  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  const build = await BuildLog.findOneByOrFail({ id: buildId });

  const statements: string[] = ['START TRANSACTION;'];

  // Build the default views
  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.toLowerCase().split('-')[0];
    const materializedViewName = `${CORE_VIEW_NAME}_mat_${lang}`;
    const originalCoreViewSQL =
      cubeBuilder.coreViewSQL.get(locale) || pgformat('SELECT * FROM %I.%I;', revisionId, FACT_TABLE_NAME);
    statements.push(
      pgformat(
        'CREATE MATERIALIZED VIEW %I.%I AS %s;',
        revisionId,
        materializedViewName,
        originalCoreViewSQL.replaceAll(build.id, revisionId)
      )
    );
    statements.push(...createViewsFromConfig(revisionId, materializedViewName, locale, viewConfig, dataset.factTable!));
    statements.push(pgformat('DROP VIEW %I.%I;', revisionId, `${CORE_VIEW_NAME}_${lang}`));
    statements.push(
      pgformat(
        `UPDATE %I.metadata SET value = %L WHERE key = %L;`,
        revisionId,
        CubeBuildStatus.Completed,
        CubeMetaDataKeys.BuildStatus
      )
    );
    statements.push(
      pgformat(
        `INSERT INTO %I.metadata VALUES(%L, %L);`,
        revisionId,
        CubeMetaDataKeys.BuildFinished,
        new Date().toISOString()
      )
    );
    const indexCols: string[] = [];
    for (const blk of cubeBuilder.transactionBlocks) {
      if (blk.indexColumns && blk.indexColumns.get(locale)) {
        indexCols.push(...blk.indexColumns.get(locale)!);
      }
    }
    for (const col of indexCols) {
      statements.push(pgformat('CREATE INDEX ON %I.%I (%I);', revisionId, materializedViewName, col));
    }
  }
  statements.push('COMMIT;');

  const fullBuildScriptArr: string[] = [];
  for (const blk of cubeBuilder.transactionBlocks) {
    fullBuildScriptArr.push(...blk.statements);
  }
  const buildStatements = statements.join('\n').replaceAll(build.id, revisionId);

  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    logger.trace(`Running query:\n\n${buildStatements}\n\n`);
    await cubeDB.query(buildStatements);
  } catch (error) {
    await cubeDB.query('ROLLBACK;');
    try {
      const errorStatements = [
        'BEGIN TRANSACTION;',
        pgformat(
          'UPDATE %I.metadata SET value = %L WHERE key = %L;',
          revisionId,
          CubeBuildStatus.Failed,
          CubeMetaDataKeys.BuildStatus
        ),
        pgformat(
          'UPDATE %I.metadata SET value = %L WHERE key = %L;',
          revisionId,
          JSON.stringify(error),
          CubeMetaDataKeys.BuildResults
        ),
        'COMMIT;'
      ];
      fullBuildScriptArr.push(...errorStatements);
      await cubeDB.query(errorStatements.join('\n'));
    } catch (err) {
      await cubeDB.query('ROLLBACK;');
      logger.error(err, 'Apparently cube no longer exists');
    }
    performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the materialized views');
    logger.error(error, 'Something went wrong trying to create the materialized views in the cube.');
    build.completeBuild(CubeBuildStatus.Failed, fullBuildScriptArr.join('\n'), JSON.stringify(error));
    await build.save();
  } finally {
    void cubeDB.release();
  }

  build.completeBuild(CubeBuildStatus.Completed, fullBuildScriptArr.join('\n'));
  await build.save();
  performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the materialized views');
}

/*
+-----------------------------------------------------------+
|  Everything below this point is internal to cube builder  |
|  and is used to build the cubes                           |
+-----------------------------------------------------------+
 */

export const makeCubeSafeString = (str: string): string => {
  return str
    .toLowerCase()
    .replace(/[ ]/g, '_')
    .replace(/[^a-zA-Z_]/g, '');
};

export function setupCubeBuilder(dataset: Dataset, buildId: string): FactTableInfo {
  if (!dataset.factTable) {
    throw new Error(`Unable to find fact table for dataset ${dataset.id}`);
  }

  const notesCodeColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.NoteCodes);
  const dataValuesColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.DataValues);
  const measureColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.Measure);

  const factTable = dataset.factTable.sort((colA, colB) => colA.columnIndex - colB.columnIndex);
  const compositeKey: string[] = [];
  const factIdentifiers: FactTableColumn[] = [];
  const factTableDef: string[] = [];

  const factTableCreationDef = factTable
    .sort((col1, col2) => col1.columnIndex - col2.columnIndex)
    .map((field) => {
      if (
        [FactTableColumnType.Measure, FactTableColumnType.Dimension, FactTableColumnType.Time].includes(
          field.columnType
        )
      ) {
        compositeKey.push(field.columnName);
        factIdentifiers.push(field);
      }
      factTableDef.push(field.columnName);
      return pgformat(
        '%I %s',
        field.columnName,
        field.columnDatatype === 'DOUBLE' ? 'DOUBLE PRECISION' : field.columnDatatype
      );
    });
  const factTableCreationQuery = pgformat(
    `CREATE TABLE %I.%I (%s);`,
    buildId,
    FACT_TABLE_NAME,
    factTableCreationDef.join(', ')
  );

  return {
    factTableCreationQuery,
    measureColumn,
    notesCodeColumn,
    dataValuesColumn,
    factTableDef,
    factIdentifiers,
    compositeKey
  };
}

function createCubeBaseTables(revisionId: string, buildId: string, factTableQuery: string): TransactionBlock {
  const statements: string[] = ['BEGIN TRANSACTION;', factTableQuery];
  statements.push(pgformat(`CREATE TABLE IF NOT EXISTS %I.metadata (key VARCHAR, value VARCHAR);`, buildId));
  statements.push(pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.Revision, revisionId));
  statements.push(pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.Build, buildId));
  statements.push(
    pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.BuildStart, new Date().toISOString())
  );
  statements.push(
    pgformat(
      'INSERT INTO %I.metadata VALUES (%L, %L);',
      buildId,
      CubeMetaDataKeys.BuildStatus,
      CubeBuildStatus.Building
    )
  );
  statements.push(
    pgformat(
      `
        CREATE TABLE %I.%I (
         reference VARCHAR,
         language VARCHAR,
         fact_table_column VARCHAR,
         dimension_name VARCHAR,
         description VARCHAR,
         hierarchy VARCHAR,
         PRIMARY KEY (reference, language, fact_table_column)
        );
      `,
      buildId,
      'filter_table'
    )
  );
  statements.push(
    pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.BuildScript, statements.join('\n'))
  );
  statements.push('COMMIT;');
  return {
    buildStage: BuildStage.BaseTables,
    statements
  };
}

function createValidationTableEntries(buildId: string, columnName: string): string {
  return pgformat(
    'SELECT DISTINCT CAST(%I AS TEXT) as reference, %L as fact_table_column FROM %I.%I',
    columnName,
    columnName,
    buildId,
    FACT_TABLE_NAME
  );
}

function setFactCountInCube(buildId: string): string {
  return pgformat(
    "INSERT INTO %I.%I (key, value) SELECT 'fact_count', COUNT(*)::text FROM %I.%I;",
    buildId,
    METADATA_TABLE_NAME,
    buildId,
    FACT_TABLE_NAME
  );
}

function setupValidationTableFromDataset(buildId: string, dataset: Dataset): TransactionBlock {
  const statements: string[] = ['BEGIN TRANSACTION;'];
  statements.push(
    pgformat(
      `CREATE TABLE %I.%I (reference TEXT, fact_table_column TEXT, PRIMARY KEY (reference, fact_table_column));`,
      buildId,
      VALIDATION_TABLE_NAME
    )
  );
  const unionParts: string[] = [];
  if (dataset.measure) {
    const measureCol = dataset.measure.factTableColumn;
    unionParts.push(createValidationTableEntries(buildId, measureCol));
  }
  for (const dim of dataset.dimensions) {
    const dimColumnName = dim.factTableColumn;
    unionParts.push(createValidationTableEntries(buildId, dimColumnName));
  }
  statements.push(pgformat('INSERT INTO %I.%I %s;', buildId, VALIDATION_TABLE_NAME, unionParts.join(' UNION ALL ')));
  statements.push(pgformat(`CREATE INDEX ON %I.%I (reference);`, buildId, VALIDATION_TABLE_NAME));
  statements.push(pgformat(`CREATE INDEX ON %I.%I (fact_table_column);`, buildId, VALIDATION_TABLE_NAME));
  statements.push(setFactCountInCube(buildId));
  statements.push('COMMIT;');

  return {
    buildStage: BuildStage.ValidationTableBuild,
    statements
  };
}

// We use this method of creating the validation table when we have no dimensions and no measure
// the reason we don't use this all the time is that there's guess work around the data and note
// code columns.
function setupValidationEntriesTableUsingRawSQL(buildId: string): TransactionBlock {
  const buildValidationStatementProc = pgformat(
    `
    DO $$
    DECLARE
      src_schema   text := %L;  -- e.g. 'public'
      src_table    text := %L;  -- e.g. 'fact_table'
      out_table    text := %L;  -- e.g. 'validation_table'
      col RECORD;
      sql TEXT := '';
    BEGIN
    FOR col IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = src_table
        AND table_schema = src_schema
        AND column_name NOT ILIKE '%%data%%'
        AND column_name NOT ILIKE '%%note%%'
    LOOP
        sql := sql || format(
            'SELECT DISTINCT CAST(%%I as TEXT) AS reference, %%L AS fact_table_column FROM %%I.%%I UNION ALL ',
            col.column_name, col.column_name, src_schema, src_table
        );
    END LOOP;

    -- Remove trailing UNION ALL
    sql := left(sql, length(sql) - 11);

    -- Create the new table
    EXECUTE format('DROP TABLE IF EXISTS %%I.%%I;', src_schema, out_table);
    EXECUTE format('CREATE TABLE %%I.%%I AS %%s;', src_schema, out_table, sql);
    EXECUTE format('CREATE INDEX ON %%I.%%I (reference);', src_schema, out_table);
    EXECUTE format('CREATE INDEX ON %%I.%%I (fact_table_column);', src_schema, out_table);
    END $$;
    `,
    buildId,
    FACT_TABLE_NAME,
    VALIDATION_TABLE_NAME
  );
  return {
    buildStage: BuildStage.ValidationTableBuild,
    statements: ['BEGIN TRANSACTION;', buildValidationStatementProc, setFactCountInCube(buildId), 'COMMIT;']
  };
}

export const loadTableDataIntoFactTableFromPostgresStatement = (
  buildId: string,
  factTableDef: string[],
  factTableName: string,
  dataTableId: string
): string => {
  return pgformat(
    'INSERT INTO %I.%I SELECT %I FROM %I.%I;',
    buildId,
    factTableName,
    factTableDef,
    'data_tables',
    dataTableId
  );
};

function createCoreCubeViewSQL(
  buildId: string,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  locale: Locale,
  joinStatements: string[],
  orderByStatements: string[]
): string {
  return pgformat(
    'SELECT %s FROM %I.%I %s %s',
    coreCubeViewSelectBuilder.get(locale)?.join(',\n'),
    buildId,
    FACT_TABLE_NAME,
    joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
    orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
  );
}

function createCubeNoSources(
  buildId: string,
  endRevision: Revision,
  factTableInfo: FactTableInfo,
  coreCubeViewSelectBuilder: Map<Locale, string[]>
): { transactionBlocks: TransactionBlock[]; coreViewSQL: Map<Locale, string> } {
  const transactionBlocks: TransactionBlock[] = [];
  const coreViewSQL = new Map<Locale, string>();
  // Create Fact Table Block
  const factTableBuildStage: TransactionBlock = {
    buildStage: BuildStage.FactTable,
    statements: []
  };
  factTableBuildStage.statements.push('BEGIN TRANSACTION;');
  factTableBuildStage.statements.push(
    loadTableDataIntoFactTableFromPostgresStatement(
      buildId,
      factTableInfo.factTableDef,
      FACT_TABLE_NAME,
      endRevision.dataTableId!
    )
  );
  factTableBuildStage.statements.push('COMMIT;');
  transactionBlocks.push(factTableBuildStage);

  transactionBlocks.push(setupValidationEntriesTableUsingRawSQL(buildId));

  // Create core view block
  const viewBuilderStage: TransactionBlock = {
    buildStage: BuildStage.CoreView,
    statements: []
  };
  viewBuilderStage.statements.push('BEGIN TRANSACTION;');
  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.toLowerCase().split('-')[0];
    const coreViewName = `${CORE_VIEW_NAME}_${lang}`;
    coreCubeViewSelectBuilder.get(locale)?.push('*');
    const coreCubeViewSQL = createCoreCubeViewSQL(buildId, coreCubeViewSelectBuilder, locale, [], []);
    coreViewSQL.set(locale, createCoreCubeViewSQL(endRevision.id, coreCubeViewSelectBuilder, locale, [], []));
    viewBuilderStage.statements.push(pgformat('CREATE VIEW %I.%I AS %s;', buildId, coreViewName, coreCubeViewSQL));
    viewBuilderStage.statements.push(
      pgformat(`INSERT INTO %I.metadata VALUES (%L, %L);`, buildId, coreViewName, coreCubeViewSQL)
    );
  }
  viewBuilderStage.statements.push(
    pgformat(
      'UPDATE %I.metadata SET value = %L WHERE key = %L;',
      buildId,
      CubeMetaDataKeys.BuildStatus,
      CubeBuildStatus.Materializing
    )
  );
  viewBuilderStage.statements.push('COMMIT;');
  transactionBlocks.push(viewBuilderStage);

  return { transactionBlocks, coreViewSQL };
}

function resetFactTable(buildId: string): string {
  return pgformat('DELETE FROM %I.%I;', buildId, FACT_TABLE_NAME);
}

function dropUpdateTable(actionId: string): string {
  return pgformat('DROP TABLE %I;', actionId);
}

function stripExistingCodes(
  buildId: string,
  tableName: string,
  notesCodeColumn: FactTableColumn,
  noteCode: NoteCode
): string {
  return pgformat(
    `UPDATE %I.%I SET %I = array_to_string(array_remove(string_to_array(replace(lower(%I.%I), ' ', ''), ','),%L),',');`,
    buildId,
    tableName,
    notesCodeColumn.columnName,
    tableName,
    notesCodeColumn.columnName,
    noteCode
  );
}

function createUpdateTable(tempTableName: string, dataTable: DataTable): string {
  return pgformat('CREATE TEMPORARY TABLE %I AS SELECT * FROM data_tables.%I;', tempTableName, dataTable.id);
}

function finaliseValues(
  buildId: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string[] {
  const statements: string[] = [];
  statements.push(
    pgformat(
      `
            UPDATE %I.%I
            SET %I = %I.%I,
                   %I = array_to_string(array_append(string_to_array(lower(%I.%I), ','), '!'), ',')
             FROM %I
             WHERE %s AND string_to_array(lower(%I.%I), ',') && string_to_array('p,f', ',');
      `,
      buildId,
      FACT_TABLE_NAME,
      dataValuesColumn.columnName,
      updateTableName,
      dataValuesColumn.columnName,
      notesCodeColumn.columnName,
      updateTableName,
      notesCodeColumn.columnName,
      updateTableName,
      joinParts.join(' AND '),
      FACT_TABLE_NAME,
      notesCodeColumn.columnName
    )
  );

  statements.push(
    pgformat(
      `UPDATE %I.%I SET %I = array_to_string(array_remove(string_to_array(%I, ','), '!'), ',');`,
      buildId,
      FACT_TABLE_NAME,
      notesCodeColumn.columnName,
      notesCodeColumn.columnName
    )
  );
  return statements;
}

function updateProvisionalAndForecastValues(
  buildId: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string[] {
  const statements: string[] = [];
  statements.push(
    pgformat(
      `UPDATE %I.%I SET %I = %I.%I, %I = %I.%I FROM %I WHERE %s AND string_to_array(%I.%I, ',') && string_to_array('p,f', ',');`,
      buildId,
      FACT_TABLE_NAME,
      dataValuesColumn.columnName,
      updateTableName,
      dataValuesColumn.columnName,
      notesCodeColumn.columnName,
      updateTableName,
      notesCodeColumn.columnName,
      updateTableName,
      joinParts.join(' AND '),
      updateTableName,
      notesCodeColumn.columnName
    )
  );
  statements.push(
    pgformat(
      `DELETE FROM %I USING %I.%I WHERE string_to_array(%I.%I, ',') && string_to_array('p,f', ',') AND %s;`,
      updateTableName,
      buildId,
      FACT_TABLE_NAME,
      updateTableName,
      notesCodeColumn.columnName,
      joinParts.join(' AND ')
    )
  );
  return statements;
}

function fixNoteCodesOnUpdateTable(
  buildId: string,
  updateTableName: string,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string[] {
  return [
    pgformat(
      `UPDATE %I SET %I = array_to_string(array_remove(string_to_array(replace(lower(%I.%I), ' ', ''), ','),%L),',');`,
      updateTableName,
      notesCodeColumn.columnName,
      updateTableName,
      notesCodeColumn.columnName,
      NoteCode.Revised
    ),
    pgformat(
      `UPDATE %I SET %I = array_to_string(array_append(array_remove(string_to_array(lower(%I.%I), ','), %L), %L), ',') FROM %I.%I WHERE %s;`,
      updateTableName,
      notesCodeColumn.columnName,
      updateTableName,
      notesCodeColumn.columnName,
      NoteCode.Revised,
      NoteCode.Revised,
      buildId,
      FACT_TABLE_NAME,
      joinParts.join(' AND ')
    )
  ];
}

function updateFactsTableFromUpdateTable(
  buildId: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string {
  return pgformat(
    `UPDATE %I.%I SET %I = %I.%I, %I = %I.%I FROM %I WHERE %s;`,
    buildId,
    FACT_TABLE_NAME,
    dataValuesColumn.columnName,
    updateTableName,
    dataValuesColumn.columnName,
    notesCodeColumn.columnName,
    updateTableName,
    notesCodeColumn.columnName,
    updateTableName,
    joinParts.join(' AND ')
  );
}

function copyUpdateTableToFactTable(
  buildId: string,
  updateTableName: string,
  factTableDef: string[],
  joinParts: string[],
  dataTableIdentifiers: DataTableDescription[]
): string[] {
  const statements: string[] = [];
  const dataTableSelect: string[] = [];
  for (const col of factTableDef) {
    const dataTableCol = dataTableIdentifiers.find((dataTableCol) => dataTableCol.factTableColumn === col);
    if (dataTableCol) dataTableSelect.push(dataTableCol.factTableColumn);
  }
  // First remove values which already exist in the fact table
  statements.push(
    pgformat(`DELETE FROM %I.%I USING %I WHERE %s;`, buildId, FACT_TABLE_NAME, updateTableName, joinParts.join(' AND '))
  );
  // Now copy over anything else which remains
  statements.push(
    pgformat(
      'INSERT INTO %I.%I (%I) (SELECT %I FROM %I);',
      buildId,
      FACT_TABLE_NAME,
      factTableDef,
      dataTableSelect,
      updateTableName
    )
  );
  return statements;
}

export function cleanupNotesCodeColumn(buildId: string, notesCodeColumn: FactTableColumn): string {
  return pgformat(
    `UPDATE %I.%I SET %I = NULL WHERE %I = '';`,
    buildId,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName,
    notesCodeColumn.columnName
  );
}

function loadFactTableFromEarlierRevision(buildId: string, previousRevisionId: string): string[] {
  return [
    pgformat('DROP TABLE IF EXISTS %I.%I;', buildId, FACT_TABLE_NAME),
    pgformat(
      'CREATE TABLE %I.%I AS SELECT * FROM %I.%I;',
      buildId,
      FACT_TABLE_NAME,
      previousRevisionId,
      FACT_TABLE_NAME
    )
  ];
}

export function dataTableActions(
  buildId: string,
  dataTable: DataTable,
  factTableDef: string[],
  notesCodeColumn: FactTableColumn,
  dataValuesColumn: FactTableColumn,
  factIdentifiers: FactTableColumn[]
): string[] {
  const buildStatements: string[] = [];
  const actionID = crypto.randomUUID();
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTable.dataTableDescriptions.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(
      pgformat(
        'CAST(%I.%I AS VARCHAR) = CAST(%I.%I AS VARCHAR)',
        FACT_TABLE_NAME,
        factTableCol.columnName,
        actionID,
        dataTableCol?.columnName
      )
    );
  }
  logger.debug(`Performing action ${dataTable.action} on fact table for data table ${dataTable.id}`);
  switch (dataTable.action) {
    case DataTableAction.ReplaceAll:
      buildStatements.push(resetFactTable(buildId));
      buildStatements.push(
        loadTableDataIntoFactTableFromPostgresStatement(buildId, factTableDef, FACT_TABLE_NAME, dataTable.id)
      );
      break;
    case DataTableAction.Add:
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Provisional));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Forecast));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Revised));
      buildStatements.push(
        loadTableDataIntoFactTableFromPostgresStatement(buildId, factTableDef, FACT_TABLE_NAME, dataTable.id)
      );
      break;
    case DataTableAction.Revise:
      buildStatements.push(createUpdateTable(actionID, dataTable));
      buildStatements.push(...finaliseValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Provisional));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Forecast));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Revised));
      buildStatements.push(
        ...updateProvisionalAndForecastValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
      );
      buildStatements.push(...fixNoteCodesOnUpdateTable(buildId, actionID, notesCodeColumn, joinParts));
      buildStatements.push(
        updateFactsTableFromUpdateTable(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
      );
      buildStatements.push(dropUpdateTable(actionID));
      break;
    case DataTableAction.AddRevise:
      buildStatements.push(createUpdateTable(actionID, dataTable));
      buildStatements.push(...finaliseValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Provisional));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Forecast));
      buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Revised));
      buildStatements.push(
        ...updateProvisionalAndForecastValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
      );
      buildStatements.push(...fixNoteCodesOnUpdateTable(buildId, actionID, notesCodeColumn, joinParts));
      buildStatements.push(
        updateFactsTableFromUpdateTable(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
      );
      buildStatements.push(
        ...copyUpdateTableToFactTable(buildId, actionID, factTableDef, joinParts, dataTable.dataTableDescriptions)
      );
      buildStatements.push(dropUpdateTable(actionID));
      break;
    case DataTableAction.Correction:
      buildStatements.push(createUpdateTable(actionID, dataTable));
      buildStatements.push(
        updateFactsTableFromUpdateTable(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
      );
      buildStatements.push(dropUpdateTable(actionID));
      break;
  }
  return buildStatements;
}

function loadFactTableFromPreviousRevision(
  buildRevision: Revision,
  buildId: string,
  factTableDef: string[],
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  factIdentifiers: FactTableColumn[],
  factTableCompositeKey: string[]
): TransactionBlock {
  const buildStatements: string[] = ['BEGIN TRANSACTION;'];
  if (buildRevision.previousRevisionId) {
    logger.debug('Previous revision present... Loading previous fact table into new cube');
    buildStatements.push(...loadFactTableFromEarlierRevision(buildId, buildRevision.previousRevisionId));
  }
  const dataTable = buildRevision.dataTable;
  if (!dataTable) {
    logger.debug('This revision has no data table attached');
    buildStatements.push('COMMIT');
    return {
      buildStage: BuildStage.FactTable,
      statements: buildStatements
    };
  }
  buildStatements.push(
    ...dataTableActions(buildId, dataTable, factTableDef, notesCodeColumn, dataValuesColumn, factIdentifiers)
  );
  buildStatements.push(cleanupNotesCodeColumn(buildId, notesCodeColumn));
  buildStatements.push(createPrimaryKeyOnFactTable(buildId, factTableCompositeKey));
  buildStatements.push('COMMIT;');

  return {
    buildStage: BuildStage.FactTable,
    statements: buildStatements
  };
}

export function createPrimaryKeyOnFactTable(buildId: string, compositeKey: string[]): string {
  return pgformat('ALTER TABLE %I.%I ADD PRIMARY KEY (%I);', buildId, FACT_TABLE_NAME, compositeKey);
}

function setupDataValueViews(
  locale: Locale,
  viewConfig: CubeViewBuilder[],
  dataValuesColumnName: string,
  dataAnnotatedColName: string,
  dataFormattedColName: string,
  dataSortColName: string
): void {
  for (const view of viewConfig) {
    if (view.config.dataValues === 'annotated') {
      view.columns.get(locale)?.add(pgformat(`%I AS %I`, dataAnnotatedColName, dataValuesColumnName));
    } else if (view.config.dataValues === 'formatted') {
      view.columns.get(locale)?.add(pgformat(`%I AS %I`, dataFormattedColName, dataValuesColumnName));
    } else {
      view.columns.get(locale)?.add(pgformat('%I', dataValuesColumnName));
    }
    if (view.config.sort_orders) {
      view.columns.get(locale)?.add(pgformat('%I', dataSortColName));
    }
  }
}

function setupMeasureViews(
  locale: Locale,
  viewConfig: CubeViewBuilder[],
  measureColumnName: string,
  measureColumnRefName: string,
  measureColumnSortName: string,
  measureColumnHierarchyName: string
): void {
  for (const view of viewConfig) {
    view.columns.get(locale)?.add(pgformat('%I', measureColumnName));
    if (view.config.refcodes) {
      view.columns.get(locale)?.add(pgformat('%I', measureColumnRefName));
    }
    if (view.config.sort_orders) {
      view.columns.get(locale)?.add(pgformat('%I', measureColumnSortName));
    }
    if (view.config.hierarchies) {
      view.columns.get(locale)?.add(pgformat('%I', measureColumnHierarchyName));
    }
  }
}

function setupMeasureAndDataValuesNoLookup(
  buildId: string,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeViewBuilder[],
  measureColumn: FactTableColumn,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn
): TransactionBlock {
  const statements: string[] = ['BEGIN TRANSACTION;'];
  const indexColumns = new Map<Locale, string[]>();
  SUPPORTED_LOCALES.map((locale) => {
    // Set up data values column (with no measure lookup)
    const dataValuesColumnName = t('column_headers.data_values', { lng: locale });
    const dataFormattedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.formatted', { lng: locale })}`;
    const dataAnnotatedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.annotated', { lng: locale })}`;
    const dataSortColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.sort', { lng: locale })}`;
    columnNames.get(locale)?.add(dataValuesColumnName);
    columnNames.get(locale)?.add(dataFormattedColName);
    columnNames.get(locale)?.add(dataAnnotatedColName);
    columnNames.get(locale)?.add(dataSortColName);
    columnNames.get(locale)?.add(t('column_headers.data_values', { lng: locale }));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataValuesColumnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataFormattedColName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataSortColName));
    if (notesCodeColumn) {
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(
          pgformat(
            `CASE WHEN %I.%I IS NULL THEN CAST(%I.%I AS VARCHAR) ELSE %I.%I || ' [' || array_to_string(string_to_array(%I.%I, ','), '] [') || ']' END AS %I`,
            FACT_TABLE_NAME,
            notesCodeColumn.columnName,
            FACT_TABLE_NAME,
            dataValuesColumn.columnName,
            FACT_TABLE_NAME,
            dataValuesColumn.columnName,
            FACT_TABLE_NAME,
            notesCodeColumn.columnName,
            dataAnnotatedColName
          )
        );
    } else {
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataAnnotatedColName));
    }
    setupDataValueViews(
      locale,
      viewConfig,
      dataValuesColumnName,
      dataAnnotatedColName,
      dataFormattedColName,
      dataSortColName
    );
    // Set up measure column (no lookup)
    const measureColumnName = t('column_headers.measure', { lng: locale });
    const measureColumnRefName = `${measureColumnName}_${t('column_headers.reference', { lng: locale })}`;
    const measureColumnSortName = `${measureColumnName}_${t('column_headers.sort', { lng: locale })}`;
    const measureColumnHierarchyName = `${measureColumnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    indexColumns.set(locale, [measureColumnRefName, measureColumnSortName, measureColumnHierarchyName]);
    columnNames.get(locale)?.add(measureColumnName);
    columnNames.get(locale)?.add(measureColumnRefName);
    columnNames.get(locale)?.add(measureColumnSortName);
    columnNames.get(locale)?.add(measureColumnHierarchyName);
    columnNames.get(locale)?.add(t('column_headers.measure', { lng: locale }));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, measureColumn.columnName, measureColumnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, measureColumn.columnName, measureColumnRefName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, measureColumn.columnName, measureColumnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('NULL AS %I', measureColumnHierarchyName));
    setupMeasureViews(
      locale,
      viewConfig,
      measureColumnName,
      measureColumnRefName,
      measureColumnSortName,
      measureColumnHierarchyName
    );
    statements.push(
      pgformat(
        `INSERT INTO %I.filter_table SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST(%I AS VARCHAR), null FROM %I.%I ORDER BY %I;`,
        buildId,
        measureColumn.columnName,
        locale.toLowerCase(),
        measureColumn.columnName,
        measureColumnName,
        measureColumn.columnName,
        buildId,
        FACT_TABLE_NAME,
        measureColumn.columnName
      )
    );
  });
  statements.push('COMMIT;');
  return {
    buildStage: BuildStage.Measure,
    statements,
    indexColumns
  };
}

export const measureTableCreateStatement = (
  joinColumnType: string,
  buildId?: string,
  tableName = 'measure'
): string => {
  if (buildId) {
    tableName = pgformat('%I.%I', buildId, tableName);
  }
  return pgformat(
    `
      CREATE TABLE %s (
                        reference %s,
                        language TEXT,
                        description TEXT,
                        notes TEXT,
                        sort_order INTEGER,
                        format TEXT,
                        decimals INTEGER,
                        measure_type TEXT,
                        hierarchy %s
      );
    `,
    tableName,
    joinColumnType,
    joinColumnType
  );
};

export function createMeasureLookupTable(
  buildId: string,
  measureColumn: FactTableColumn,
  measureTable: MeasureRow[]
): string[] {
  const statements: string[] = [];
  statements.push(measureTableCreateStatement(measureColumn.columnDatatype, buildId));
  for (const row of measureTable) {
    const values = [
      row.reference,
      row.language.toLowerCase(),
      row.description,
      row.notes ? row.notes : null,
      row.sortOrder ? row.sortOrder : null,
      row.format,
      row.decimal ? row.decimal : null,
      row.measureType ? row.measureType : null,
      row.hierarchy ? row.hierarchy : null
    ];
    statements.push(pgformat('INSERT INTO %I.measure VALUES (%L);', buildId, values));
  }
  return statements;
}

function toUniqueMeasureDetailsByRef(measureTable: MeasureRow[]): UniqueMeasureDetails[] {
  const map = new Map<string, UniqueMeasureDetails>();
  for (const r of measureTable) {
    if (!map.has(r.reference)) {
      map.set(r.reference, {
        reference: r.reference,
        format: r.format,
        sort_order: r.sortOrder ?? null,
        decimals: r.decimal ?? null
      });
    }
  }
  return Array.from(map.values());
}

function postgresMeasureFormats(): Map<string, MeasureFormat> {
  const measureFormats: Map<string, MeasureFormat> = new Map();
  measureFormats.set('decimal', {
    name: 'decimal',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('float', {
    name: 'float',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('integer', {
    name: 'integer',
    method: "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(CAST(|COL| AS BIGINT), '999,999,990'))"
  });
  measureFormats.set('long', {
    name: 'long',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('percentage', {
    name: 'percentage',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('string', {
    name: 'string',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('text', {
    name: 'text',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('date', {
    name: 'date',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('datetime', {
    name: 'datetime',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('time', {
    name: 'time',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  return measureFormats;
}

function setupMeasureAndDataValuesWithLookup(
  buildId: string,
  measureTable: MeasureRow[],
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn | undefined,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): TransactionBlock {
  const statements: string[] = ['BEGIN TRANSACTION;'];
  const indexColumns = new Map<Locale, string[]>();
  statements.push(...createMeasureLookupTable(buildId, measureColumn, measureTable));

  logger.debug('Creating query part to format the data value correctly');

  const uniqueReferences = toUniqueMeasureDetailsByRef(measureTable);
  const caseStatements: string[] = ['CASE'];
  for (const row of uniqueReferences) {
    const statement = postgresMeasureFormats()
      .get(row.format.toLowerCase())
      ?.method.replace('|REF|', pgformat('%L', row.reference))
      .replace('|DEC|', row.decimals ? `${row.decimals}` : '0')
      .replace('|ZEROS|', row.decimals ? `.${'0'.repeat(row.decimals)}` : '')
      .replace('|COL|', pgformat('%I.%I', FACT_TABLE_NAME, dataValuesColumn.columnName));
    if (statement) {
      caseStatements.push(statement);
    } else {
      logger.warn(`Failed to create case statement measure row: ${JSON.stringify(row)}`);
    }
  }
  caseStatements.push(pgformat('ELSE CAST(%I.%I AS VARCHAR) END', FACT_TABLE_NAME, dataValuesColumn?.columnName));
  SUPPORTED_LOCALES.map((locale) => {
    if (dataValuesColumn) {
      const dataValuesColumnName = t('column_headers.data_values', { lng: locale });
      const dataFormattedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.formatted', { lng: locale })}`;
      const dataAnnotatedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.annotated', { lng: locale })}`;
      const dataSortColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.sort', { lng: locale })}`;
      columnNames.get(locale)?.add(dataValuesColumnName);
      columnNames.get(locale)?.add(dataFormattedColName);
      columnNames.get(locale)?.add(dataAnnotatedColName);
      columnNames.get(locale)?.add(dataSortColName);
      // Add all variations of the column to the core or extended view of the dataset
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataValuesColumnName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat(`%s AS %I`, caseStatements.join('\n'), dataFormattedColName));
      if (notesCodeColumn) {
        coreCubeViewSelectBuilder
          .get(locale)
          ?.push(
            pgformat(
              `CASE WHEN %I.%I IS NULL THEN %s ELSE %s || ' [' || array_to_string(string_to_array(lower(%I.%I), ','), '] [') || ']' END AS %I`,
              FACT_TABLE_NAME,
              notesCodeColumn.columnName,
              caseStatements.join('\n'),
              caseStatements.join('\n'),
              FACT_TABLE_NAME,
              notesCodeColumn.columnName,
              dataAnnotatedColName
            )
          );
        coreCubeViewSelectBuilder
          .get(locale)
          ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataSortColName));
      } else {
        coreCubeViewSelectBuilder
          .get(locale)
          ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataAnnotatedColName));
      }
      setupDataValueViews(
        locale,
        viewConfig,
        dataValuesColumnName,
        dataAnnotatedColName,
        dataFormattedColName,
        dataSortColName
      );
    }
    const measureColumnName = t('column_headers.measure', { lng: locale });
    const measureColumnRefName = `${measureColumnName}_${t('column_headers.reference', { lng: locale })}`;
    const measureColumnSortName = `${measureColumnName}_${t('column_headers.sort', { lng: locale })}`;
    const measureColumnHierarchyName = `${measureColumnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    indexColumns.set(locale, [measureColumnRefName, measureColumnSortName, measureColumnHierarchyName]);
    columnNames.get(locale)?.add(measureColumnName);
    columnNames.get(locale)?.add(measureColumnRefName);
    columnNames.get(locale)?.add(measureColumnSortName);
    columnNames.get(locale)?.add(measureColumnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.description AS %I', measureColumnName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.reference AS %I', `${measureColumnRefName}`));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.sort_order AS %I', `${measureColumnSortName}`));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.hierarchy AS %I', `${measureColumnHierarchyName}`));
    setupMeasureViews(
      locale,
      viewConfig,
      measureColumnName,
      measureColumnRefName,
      measureColumnSortName,
      measureColumnHierarchyName
    );
  });
  joinStatements.push(
    pgformat(
      'LEFT JOIN %I.measure on measure.reference=%I.%I AND measure.language=#LANG#',
      buildId,
      FACT_TABLE_NAME,
      measureColumn.columnName
    )
  );
  orderByStatements.push(`measure.sort_order, measure.reference`);
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = t('column_headers.measure', { lng: locale });
    statements.push(
      pgformat(
        `INSERT INTO %I.filter_table SELECT CAST(reference AS VARCHAR), language, %L, %L, description, CAST(hierarchy AS VARCHAR) FROM %I.measure WHERE language = %L ORDER BY sort_order, reference;`,
        buildId,
        measureColumn.columnName,
        columnName,
        buildId,
        locale.toLowerCase()
      )
    );
  }
  statements.push('COMMIT;');
  return {
    buildStage: BuildStage.Measure,
    statements,
    indexColumns
  };
}

function rawDimensionProcessor(
  buildId: string,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  indexColumns: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[]
): string[] {
  const statements: string[] = [];
  for (const locale of SUPPORTED_LOCALES) {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    const cols = indexColumns.get(locale) || [];
    cols.push(...[columnRefName, columnSortName, columnHierarchyName]);
    indexColumns.set(locale, cols);
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnRefName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('NULL AS %I', columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
    statements.push(
      pgformat(
        `INSERT INTO %I.filter_table
       SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
       FROM %I.%I ORDER BY %I;`,
        buildId,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        buildId,
        FACT_TABLE_NAME,
        dimension.factTableColumn
      )
    );
  }
  return statements;
}

export function createLookupTableDimension(
  buildId: string,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): string {
  logger.debug(`Creating and validating lookup table dimension ${dimension.factTableColumn}`);
  const dimTable = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  return pgformat(
    'CREATE TABLE %I.%I AS SELECT * FROM lookup_tables.%I;',
    buildId,
    dimTable,
    dimension.lookupTable!.id
  );
}

function updateColumnName(existingColumnNames: Set<string>, proposedColumnName: string): string {
  let columnName = proposedColumnName;
  let count = 1;
  while (existingColumnNames.has(columnName)) {
    columnName = `${proposedColumnName}_${count}`;
    count++;
  }
  return columnName;
}

function setupLookupTableDimension(
  buildId: string,
  dataset: Dataset,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  indexColumns: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[],
  viewConfig: CubeViewBuilder[]
): string[] {
  const statements: string[] = [];
  const factTableColumn = dataset.factTable?.find((col) => col.columnName === dimension.factTableColumn);
  if (!factTableColumn) {
    const error = new CubeValidationException(`Fact table column ${dimension.factTableColumn} not found`);
    error.type = CubeValidationType.FactTableColumnMissing;
    error.datasetId = dataset.id;
    throw error;
  }
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  statements.push(createLookupTableDimension(buildId, dimension, factTableColumn));

  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    const cols = indexColumns.get(locale) || [];
    cols.push(...[columnRefName, columnSortName, columnHierarchyName]);
    indexColumns.set(locale, cols);
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat(`%I.description AS %I`, dimTable, columnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat(`%I.%I AS %I`, dimTable, factTableColumn.columnName, columnRefName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat(`%I.sort_order AS %I`, dimTable, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat(`%I.hierarchy AS %I`, dimTable, columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
  });
  joinStatements.push(
    pgformat(
      `LEFT JOIN %I.%I on %I.%I=%I.%I AND %I.language=#LANG#`,
      buildId,
      dimTable,
      dimTable,
      factTableColumn.columnName,
      FACT_TABLE_NAME,
      factTableColumn.columnName,
      dimTable
    )
  );

  if (dimension.type === DimensionType.DatePeriod || dimension.type === DimensionType.Date) {
    orderByStatements.push(pgformat('%I.sort_order DESC', dimTable));
  } else {
    orderByStatements.push(pgformat('%I.sort_order', dimTable));
  }

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    statements.push(
      pgformat(
        `INSERT INTO %I.filter_table
              SELECT reference, language, fact_table_column, dimension_name, description, hierarchy
              FROM (SELECT DISTINCT
              CAST(%I AS VARCHAR) AS reference, language, %L AS fact_table_column, %L AS dimension_name, description, hierarchy, sort_order
            FROM %I.%I
            WHERE language = %L
            ORDER BY sort_order, description);`,
        buildId,
        dimension.factTableColumn,
        dimension.factTableColumn,
        columnName,
        buildId,
        dimTable,
        locale.toLowerCase(),
        dimension.factTableColumn
      )
    );
  }
  return statements;
}

function setupNumericDimension(
  buildId: string,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  indexColumns: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[]
): string[] {
  const statements: string[] = [];
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    const cols = indexColumns.get(locale) || [];
    cols.push(...[columnRefName, columnSortName, columnHierarchyName]);
    indexColumns.set(locale, cols);
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    if ((dimension.extractor as NumberExtractor).type === NumberType.Integer) {
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnRefName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnSortName));
    } else {
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(
          pgformat(
            `format('%%s', TO_CHAR(ROUND(CAST(%I.%I AS DECIMAL), %L), '999,999,990.%s')) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnName
          )
        );
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(
          pgformat(
            `CAST(%I.%I AS DECIMAL) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnRefName
          )
        );
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(
          pgformat(
            `CAST(%I.%I AS DECIMAL) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnSortName
          )
        );
    }
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('NULL AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
  });
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    statements.push(
      pgformat(
        `INSERT INTO %I.filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I.%I ORDER BY %I`,
        buildId,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        buildId,
        FACT_TABLE_NAME,
        dimension.factTableColumn
      )
    );
  }
  return statements;
}

function setupTextDimension(
  buildId: string,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  indexColumns: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[]
): string[] {
  const statements: string[] = [];
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    const cols = indexColumns.get(locale) || [];
    cols.push(...[columnRefName, columnSortName, columnHierarchyName]);
    indexColumns.set(locale, cols);
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnRefName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('NULL AS %I', columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
  });

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    statements.push(
      pgformat(
        `INSERT INTO %I.filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I.%I`,
        buildId,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        buildId,
        FACT_TABLE_NAME
      )
    );
  }
  return statements;
}

function setupDimensions(
  buildId: string,
  dataset: Dataset,
  endRevision: Revision,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): TransactionBlock {
  logger.info('Setting up dimension tables...');
  const statements: string[] = ['BEGIN TRANSACTION;'];
  const lookupTables: Set<string> = new Set<string>();
  const indexColumns = new Map<Locale, string[]>();
  for (const locale of SUPPORTED_LOCALES) {
    indexColumns.set(locale, []);
  }
  const factTable = dataset.factTable!;

  const orderedDimension = dataset.dimensions.map((dim) => {
    const col = factTable.find((col) => col.columnName === dim.factTableColumn);
    return {
      dimension: dim,
      index: col ? factTable.indexOf(col) : -1
    };
  });
  for (const dim of orderedDimension.sort((dimA, dimB) => dimA.index - dimB.index)) {
    const dimStart = performance.now();
    const dimension = dim.dimension;
    const tableName = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
    const factTableColumn = dataset.factTable?.find(
      (col) =>
        col.columnName === dimension.factTableColumn &&
        (col.columnType === FactTableColumnType.Dimension || col.columnType === FactTableColumnType.Unknown)
    );
    if (!factTableColumn) {
      const error = new CubeValidationException(
        `No fact table column found for dimension ${dimension.id} in dataset ${dataset.id}`
      );
      error.type = CubeValidationType.FactTableColumnMissing;
      throw error;
    }
    logger.info(`Setting up dimension ${dimension.id} for fact table column ${dimension.factTableColumn}`);
    if (
      endRevision.tasks &&
      endRevision.tasks.dimensions.find((dim) => dim.id === dimension.id && !dim.lookupTableUpdated)
    ) {
      dimension.type = DimensionType.Raw;
    }

    try {
      switch (dimension.type) {
        case DimensionType.DatePeriod:
        case DimensionType.Date:
        case DimensionType.LookupTable:
          statements.push(
            ...setupLookupTableDimension(
              buildId,
              dataset,
              dimension,
              coreCubeViewSelectBuilder,
              columnNames,
              indexColumns,
              joinStatements,
              orderByStatements,
              viewConfig
            )
          );
          lookupTables.add(tableName);
          break;
        case DimensionType.Numeric:
          statements.push(
            ...setupNumericDimension(
              buildId,
              dimension,
              coreCubeViewSelectBuilder,
              columnNames,
              indexColumns,
              viewConfig
            )
          );
          break;
        case DimensionType.Text:
          statements.push(
            ...setupTextDimension(buildId, dimension, coreCubeViewSelectBuilder, columnNames, indexColumns, viewConfig)
          );
          break;
        case DimensionType.Raw:
        case DimensionType.Symbol:
          statements.push(
            ...rawDimensionProcessor(
              buildId,
              dimension,
              coreCubeViewSelectBuilder,
              columnNames,
              indexColumns,
              viewConfig
            )
          );
          break;
      }
    } catch (err) {
      logger.error(err, `Something went wrong trying to load dimension ${dimension.id} in to the cube`);
      throw new Error(`Could not load dimensions ${dimension.id} in to the cube with the following error: ${err}`);
    }
    performanceReporting(Math.round(performance.now() - dimStart), 1000, `Setting up ${dimension.type} dimension type`);
  }
  statements.push(
    pgformat(
      'INSERT INTO %I.metadata VALUES (%L, %L);',
      buildId,
      CubeMetaDataKeys.LookupTables,
      JSON.stringify(Array.from(lookupTables))
    )
  );
  statements.push('COMMIT;');
  return {
    buildStage: BuildStage.Dimensions,
    statements,
    indexColumns
  };
}

function setupMeasuresAndDataValues(
  buildId: string,
  dataset: Dataset,
  revision: Revision,
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): TransactionBlock {
  let createMeasureTable = true;
  if (revision.tasks && revision.tasks.measure) {
    createMeasureTable = false;
  }

  if (createMeasureTable && dataset.measure.measureTable && dataset.measure.measureTable.length > 0) {
    return setupMeasureAndDataValuesWithLookup(
      buildId,
      dataset.measure.measureTable,
      dataValuesColumn,
      measureColumn,
      notesCodeColumn,
      coreCubeViewSelectBuilder,
      viewConfig,
      columnNames,
      joinStatements,
      orderByStatements
    );
  }

  return setupMeasureAndDataValuesNoLookup(
    buildId,
    coreCubeViewSelectBuilder,
    columnNames,
    viewConfig,
    measureColumn,
    dataValuesColumn,
    notesCodeColumn
  );
}

function createNotesTable(
  buildId: string,
  notesColumn: FactTableColumn,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  viewConfig: CubeViewBuilder[]
): TransactionBlock {
  const statements: string[] = ['BEGIN TRANSACTION;'];
  logger.info('Creating notes table...');

  statements.push(
    pgformat(
      `CREATE TABLE %I.note_codes (code VARCHAR, language VARCHAR, tag VARCHAR, description VARCHAR, notes VARCHAR);`,
      buildId
    )
  );
  for (const locale of SUPPORTED_LOCALES) {
    for (const noteCode of NoteCodes) {
      statements.push(
        pgformat('INSERT INTO %I.note_codes (code, language, tag, description, notes) VALUES (%L);', buildId, [
          noteCode.code,
          locale.toLowerCase(),
          noteCode.tag,
          t(`note_codes.${noteCode.tag}`, { lng: locale }),
          null
        ])
      );
    }
  }
  logger.info('Creating notes table view...');
  // We perform join operations to this view as we want to turn a csv such as `a,r` in to `Average, Revised`.
  statements.push(
    pgformat(
      `CREATE TABLE %I.all_notes AS SELECT fact_table.%I as code, note_codes.language as language, string_agg(DISTINCT note_codes.description, ', ') as description
          FROM %I.fact_table JOIN %I.note_codes ON array_position(string_to_array(fact_table.%I, ','), note_codes.code) IS NOT NULL
          GROUP BY fact_table.%I, note_codes.language;`,
      buildId,
      notesColumn.columnName,
      buildId,
      buildId,
      notesColumn.columnName,
      notesColumn.columnName
    )
  );

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = t('column_headers.notes', { lng: locale });
    columnNames.get(locale)?.add(columnName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('all_notes.description AS %I', columnName));
    for (const view of viewConfig) {
      if (view.config.note_descriptions) {
        view.columns.get(locale)?.add(pgformat('%I', columnName));
      }
    }
  }
  joinStatements.push(
    pgformat(
      `LEFT JOIN %I.all_notes on all_notes.code=fact_table.%I AND all_notes.language=#LANG#`,
      buildId,
      notesColumn.columnName
    )
  );
  statements.push(
    pgformat(
      `INSERT INTO %I.metadata VALUES ('note_codes', (SELECT ARRAY_TO_STRING(ARRAY(SELECT DISTINCT unnest(string_to_array(%I, ',')) from %I.%I WHERE %I IS NOT NULL), ',') AS note_codes));`,
      buildId,
      notesColumn.columnName,
      buildId,
      FACT_TABLE_NAME,
      notesColumn.columnName
    )
  );
  statements.push('COMMIT;');

  return { buildStage: BuildStage.NoteCodes, statements };
}

function createViewsFromConfig(
  buildId: string,
  baseViewName: string,
  locale: Locale,
  viewConfig: CubeViewBuilder[],
  factTable: FactTableColumn[]
): string[] {
  const statements: string[] = [];
  const lang = locale.toLowerCase().split('-')[0];
  for (const view of viewConfig) {
    const viewName = `${view.name}_${lang}`;
    let cols: string[] = [];
    if (Array.from(view.columns.get(locale)?.values() || []).length > 0) {
      cols = Array.from(view.columns.get(locale)!.values());
    } else {
      cols = factTable.map((col) => pgformat('%I', col.columnName)) || ['*'];
    }
    const SQL = pgformat('SELECT %s FROM %I.%I', cols.join(', '), buildId, baseViewName);
    statements.push(pgformat('DELETE FROM %I.metadata WHERE key = %L;', buildId, viewName));
    statements.push(pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, viewName, SQL));
    statements.push(pgformat('DELETE FROM %I.metadata WHERE key = %L;', buildId, `${viewName}_columns`));
    statements.push(
      pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, `${viewName}_columns`, JSON.stringify(cols))
    );
    statements.push(pgformat('DROP VIEW IF EXISTS %I.%I;', buildId, viewName));
    statements.push(pgformat('CREATE VIEW %I.%I AS %s;', buildId, viewName, SQL));
  }
  return statements;
}

function createValidationCube(
  buildId: string,
  dataset: Dataset,
  endRevision: Revision,
  factTableInfo: FactTableInfo,
  coreCubeViewSelectBuilder: Map<Locale, string[]>
): { transactionBlocks: TransactionBlock[]; coreViewSQLMap: Map<Locale, string> } {
  const transactionBlocks: TransactionBlock[] = [];
  const coreViewSQLMap: Map<Locale, string> = new Map();

  const columnNames = new Map<Locale, Set<string>>();
  SUPPORTED_LOCALES.map((locale) => {
    columnNames.set(locale, new Set<string>());
  });

  transactionBlocks.push(
    loadFactTableFromPreviousRevision(
      endRevision,
      buildId,
      factTableInfo.factTableDef,
      factTableInfo.dataValuesColumn!,
      factTableInfo.notesCodeColumn!,
      factTableInfo.factIdentifiers,
      factTableInfo.compositeKey
    )
  );

  transactionBlocks.push(setupValidationTableFromDataset(buildId, dataset));

  // Create core view block
  const viewBuilderStage: TransactionBlock = {
    buildStage: BuildStage.CoreView,
    statements: []
  };
  viewBuilderStage.statements.push('BEGIN TRANSACTION;');
  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.toLowerCase().split('-')[0];
    const coreViewName = `${CORE_VIEW_NAME}_${lang}`;
    coreCubeViewSelectBuilder.get(locale)?.push('*');
    const coreCubeViewSQL = createCoreCubeViewSQL(buildId, coreCubeViewSelectBuilder, locale, [], []);
    coreViewSQLMap.set(locale, coreCubeViewSQL);
    viewBuilderStage.statements.push(pgformat('CREATE VIEW %I.%I AS %s;', buildId, coreViewName, coreCubeViewSQL));
    viewBuilderStage.statements.push(
      pgformat(`INSERT INTO %I.metadata VALUES (%L, %L);`, buildId, coreViewName, coreCubeViewSQL)
    );
  }
  viewBuilderStage.statements.push(
    pgformat(
      'UPDATE %I.metadata SET value = %L WHERE key = %L;',
      buildId,
      CubeMetaDataKeys.BuildStatus,
      CubeBuildStatus.Materializing
    )
  );
  viewBuilderStage.statements.push('COMMIT;');
  transactionBlocks.push(viewBuilderStage);

  return { transactionBlocks, coreViewSQLMap };
}

function createFullCube(
  buildId: string,
  dataset: Dataset,
  buildRevision: Revision,
  factTableInfo: FactTableInfo,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[]
): { transactionBlocks: TransactionBlock[]; coreViewSQLMap: Map<Locale, string> } {
  const transactionBlocks: TransactionBlock[] = [];
  const coreViewSQLMap: Map<Locale, string> = new Map();

  const columnNames = new Map<Locale, Set<string>>();
  SUPPORTED_LOCALES.map((locale) => {
    columnNames.set(locale, new Set<string>());
  });

  transactionBlocks.push(
    loadFactTableFromPreviousRevision(
      buildRevision,
      buildId,
      factTableInfo.factTableDef,
      factTableInfo.dataValuesColumn!,
      factTableInfo.notesCodeColumn!,
      factTableInfo.factIdentifiers,
      factTableInfo.compositeKey
    )
  );

  transactionBlocks.push(setupValidationTableFromDataset(buildId, dataset));

  const joinStatements: string[] = [];
  const orderByStatements: string[] = [];

  transactionBlocks.push(
    setupMeasuresAndDataValues(
      buildId,
      dataset,
      buildRevision,
      factTableInfo.dataValuesColumn!,
      factTableInfo.measureColumn!,
      factTableInfo.notesCodeColumn!,
      coreCubeViewSelectBuilder,
      viewConfig,
      columnNames,
      joinStatements,
      orderByStatements
    )
  );

  transactionBlocks.push(
    setupDimensions(
      buildId,
      dataset,
      buildRevision,
      coreCubeViewSelectBuilder,
      viewConfig,
      columnNames,
      joinStatements,
      orderByStatements
    )
  );

  transactionBlocks.push(
    createNotesTable(
      buildId,
      factTableInfo.notesCodeColumn!,
      coreCubeViewSelectBuilder,
      columnNames,
      joinStatements,
      viewConfig
    )
  );

  const viewBuildStatements: string[] = ['BEGIN TRANSACTION;'];
  for (const locale of SUPPORTED_LOCALES) {
    if (coreCubeViewSelectBuilder.get(locale)?.length === 0) {
      coreCubeViewSelectBuilder.get(locale)?.push('*');
    }
    const lang = locale.toLowerCase().split('-')[0];

    const coreViewName = `${CORE_VIEW_NAME}_${lang}`;

    const coreCubeViewSQL = createCoreCubeViewSQL(
      buildId,
      coreCubeViewSelectBuilder,
      locale,
      joinStatements,
      orderByStatements
    );
    coreViewSQLMap.set(
      locale,
      createCoreCubeViewSQL(buildRevision.id, coreCubeViewSelectBuilder, locale, joinStatements, orderByStatements)
    );

    logger.trace(`core cube view SQL: ${coreCubeViewSQL}`);
    viewBuildStatements.push(pgformat('CREATE VIEW %I.%I AS %s;', buildId, coreViewName, coreCubeViewSQL));
    viewBuildStatements.push(
      pgformat(`INSERT INTO %I.metadata VALUES (%L, %L);`, buildId, coreViewName, coreCubeViewSQL)
    );

    viewBuildStatements.push(
      pgformat(
        `INSERT INTO %I.metadata VALUES (%L, %L);`,
        buildId,
        `${CORE_VIEW_NAME}_columns_${lang}`,
        JSON.stringify(Array.from(columnNames.get(locale)?.values() || []))
      )
    );

    viewBuildStatements.push(...createViewsFromConfig(buildId, coreViewName, locale, viewConfig, dataset.factTable!));
  }
  viewBuildStatements.push(
    pgformat(
      'UPDATE %I.metadata SET value = %L WHERE key = %L;',
      buildId,
      CubeMetaDataKeys.BuildStatus,
      CubeBuildStatus.Materializing
    )
  );
  viewBuildStatements.push('COMMIT;');
  transactionBlocks.push({ buildStage: BuildStage.CoreView, statements: viewBuildStatements });

  return { transactionBlocks, coreViewSQLMap };
}
