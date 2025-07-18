import { QueryRunner } from 'typeorm';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';

import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { logger } from '../utils/logger';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { ReferenceType } from '../enums/reference-type';
import { DimensionType } from '../enums/dimension-type';

import { cleanUpDimension } from './dimension-processor';
import { createReferenceDataTablesInCube, loadReferenceDataFromCSV } from './cube-handler';
import { dbManager } from '../db/database-manager';

const sampleSize = 5;

async function setupDimension(dimension: Dimension, categories: string[]): Promise<void> {
  // Clean up previously uploaded dimensions
  if (dimension.extractor) await cleanUpDimension(dimension);
  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.type = DimensionType.ReferenceData;
  updateDimension.joinColumn = 'reference_data.item_id';
  logger.debug(`Creating extractor...`);
  updateDimension.extractor = {
    categories
  };
  logger.debug('Saving the dimension');
  await updateDimension.save();
}

async function copyAllReferenceDataIntoTable(cubeDB: QueryRunner): Promise<void> {
  logger.debug('Copying all reference data to the reference_data table.');
  await cubeDB.query(`INSERT INTO reference_data (SELECT * FROM reference_data_all);`);
}

async function validateUnknownReferenceDataItems(
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension
): Promise<ViewErrDTO | undefined> {
  const nonMatchedRowsQuery = pgformat(
    `
      SELECT fact_table.%I, reference_data.item_id FROM fact_table
      LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table.%I AS VARCHAR)
      WHERE item_id IS NULL;
    `,
    dimension.factTableColumn,
    dimension.factTableColumn
  );
  const nonMatchedRows = await cubeDB.query(nonMatchedRowsQuery);
  if (nonMatchedRows.length > 0) {
    logger.error('The user has unknown items in their reference data column');
    const nonMatchingDataTableValuesQuery = pgformat(
      `
        SELECT DISTINCT fact_table.%I, reference_data.item_id FROM fact_table
        LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table.%I AS VARCHAR)
        WHERE reference_data.item_id IS NULL;
      `,
      dimension.factTableColumn,
      dimension.factTableColumn
    );
    const nonMatchingDataTableValues = await cubeDB.query(nonMatchingDataTableValuesQuery);
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.unknown_reference_data_items', {
      totalNonMatching: nonMatchedRows.length,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      nonMatchingDataTableValues: nonMatchingDataTableValues.map((row: any) => Object.values(row)[0]),
      mismatch: true
    });
  }
  return undefined;
}

async function validateAllItemsAreInCategory(
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension,
  referenceDataType: ReferenceType,
  lang: string
): Promise<ViewErrDTO | undefined> {
  const nonMatchedRowsQuery = pgformat(
    `
      SELECT fact_table.%I, reference_data.item_id, reference_data.category_key FROM fact_table
      LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table.%I AS VARCHAR)
      JOIN category_keys ON reference_data.category_key=category_keys.category_key
      JOIN categories ON categories.category=category_keys.category
      WHERE categories.category!=%L;
    `,
    dimension.factTableColumn,
    dimension.factTableColumn,
    referenceDataType
  );
  const nonMatchedRows = await cubeDB.query(nonMatchedRowsQuery);
  if (nonMatchedRows.length > 0) {
    logger.error('The user has unknown items in their reference data column');
    const nonMatchingDataTableValuesQuery = pgformat(
      `
        SELECT fact_table.%I, first(reference_data.category_key), first(categories.category), first(category_info.description) FROM fact_table
        LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table.%I AS VARCHAR)
        JOIN category_keys ON reference_data.category_key=category_keys.category_key
        JOIN categories ON categories.category=category_keys.category JOIN category_info ON categories.category=category_info.category
        AND lang=%L
        WHERE categories.category!=%L GROUP BY fact_table.%I, item_id;
      `,
      dimension.factTableColumn,
      dimension.factTableColumn,
      lang.toLowerCase(),
      referenceDataType,
      dimension.factTableColumn
    );
    const nonMatchingDataTableValues = await cubeDB.query(nonMatchingDataTableValuesQuery);
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.items_not_in_category', {
      totalNonMatching: nonMatchingDataTableValues.length,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      nonMatchingDataTableValues: nonMatchingDataTableValues.map((row: any) => Object.values(row)[0]),
      mismatch: true
    });
  }
  return undefined;
}

async function validateAllItemsAreInOneCategory(
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension
): Promise<ViewErrDTO | string> {
  const categoriesPresent: { category: string }[] = await cubeDB.query(`
    SELECT DISTINCT categories.category as category FROM fact_table
    LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
    JOIN category_keys ON reference_data.category_key=category_keys.category_key
    JOIN categories ON categories.category=category_keys.category;
  `);
  if (categoriesPresent.length > 1) {
    logger.error('The user has more than one type of category in reference data column');
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.to_many_categories_present', {
      totalNonMatching: categoriesPresent.length,
      nonMatchingDataTableValues: categoriesPresent.map((row) => Object.values(row)[0])
    });
  }
  if (categoriesPresent.length === 0) {
    logger.error('There users column can not be matched to anything in the reference data');
    return viewErrorGenerators(
      400,
      dataset.id,
      'patch',
      'errors.dimension_validation.no_reference_data_categories_present',
      {}
    );
  }
  return categoriesPresent[0].category;
}

export const validateReferenceData = async (
  dataset: Dataset,
  dimension: Dimension,
  referenceDataType: ReferenceType | undefined,
  lang: string
): Promise<ViewDTO | ViewErrDTO> => {
  const revision = dataset.draftRevision!;
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }
  try {
    // Load reference data in to cube
    await createReferenceDataTablesInCube(revision.id);
    await loadReferenceDataFromCSV(revision.id);
    await copyAllReferenceDataIntoTable(cubeDB);
  } catch (err) {
    cubeDB.release();
    logger.error(err, `Something went wrong trying to load the reference data into the cube`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.reference_data_loading_failed', {});
  }

  let confirmedReferenceDataCategory = referenceDataType?.toString();

  try {
    logger.debug(`Validating reference data`);
    const itemsNotPresentInReferenceData = await validateUnknownReferenceDataItems(cubeDB, dataset, dimension);
    if (itemsNotPresentInReferenceData) {
      cubeDB.release();
      return itemsNotPresentInReferenceData;
    }
    if (referenceDataType) {
      const itemsOutsideOfCategory = await validateAllItemsAreInCategory(
        cubeDB,
        dataset,
        dimension,
        referenceDataType,
        lang
      );
      if (itemsOutsideOfCategory) {
        cubeDB.release();
        return itemsOutsideOfCategory;
      }
    } else {
      const referenceDataCategory = await validateAllItemsAreInOneCategory(cubeDB, dataset, dimension);
      if ((referenceDataCategory as ViewErrDTO).errors) {
        cubeDB.release();
        return referenceDataCategory as ViewErrDTO;
      }
      confirmedReferenceDataCategory = referenceDataCategory as string;
    }
  } catch (error) {
    cubeDB.release();
    logger.error(error, `Something went wrong trying to validate reference data`);
    return viewErrorGenerators(
      500,
      dataset.id,
      'patch',
      'errors.dimension_validation.reference_data_validation_failed.',
      {}
    );
  }

  const categoriesPresentQuery = pgformat(
    `
      SELECT DISTINCT category_keys.category_key FROM fact_table
      LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table.%I AS VARCHAR)
      JOIN category_keys ON reference_data.category_key=category_keys.category_key
      JOIN categories ON categories.category=category_keys.category
      WHERE categories.category=%L;
    `,
    dimension.factTableColumn,
    confirmedReferenceDataCategory
  );
  const categoriesPresent: { category_keys: string }[] = await cubeDB.query(categoriesPresentQuery);

  logger.debug(`Column passed reference data checks. Setting up dimension.`);
  await setupDimension(
    dimension,
    categoriesPresent.map((row) => Object.values(row)[0])
  );

  try {
    logger.debug('Passed validation preparing to send back the preview');
    const allMatchesQuery = `
      SELECT DISTINCT fact_table."${dimension.factTableColumn}", reference_data_info.description
      FROM fact_table
      LEFT JOIN reference_data
        ON CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)=reference_data.item_id
      JOIN reference_data_info
        ON reference_data.item_id=reference_data_info.item_id
        AND reference_data.category_key=reference_data_info.category_key
        AND reference_data.version_no=reference_data_info.version_no
      WHERE reference_data_info.lang='${lang.toLowerCase()}';
    `;
    const allMatches = await cubeDB.query(allMatchesQuery);
    const previewQuery = `
      SELECT DISTINCT fact_table."${dimension.factTableColumn}", reference_data_info.description
      FROM fact_table
      LEFT JOIN reference_data
        ON CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)=reference_data.item_id
      JOIN reference_data_info
        ON reference_data.item_id=reference_data_info.item_id
        AND reference_data.category_key=reference_data_info.category_key
        AND reference_data.version_no=reference_data_info.version_no
      WHERE reference_data_info.lang='${lang.toLowerCase()}'
      LIMIT ${sampleSize};
    `;
    // logger.debug(`Preview query = ${previewQuery}`);
    const dimensionTable = await cubeDB.query(previewQuery);
    const tableHeaders = Object.keys(dimensionTable[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = dimensionTable.map((row: any) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id, { dimensions: { metadata: true } });
    const headers: CSVHeader[] = tableHeaders.map((header, index) => {
      return {
        index,
        name: header,
        source_type: FactTableColumnType.Unknown
      };
    });
    const pageInfo = {
      total_records: allMatches.length,
      start_record: 1,
      end_record: dataArray.length
    };
    const pageSize = dataArray.length < sampleSize ? dataArray.length : sampleSize;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the column`);
    return viewErrorGenerators(
      500,
      dataset.id,
      'patch',
      'errors.dimension_validation.reference_data_preview_failed',
      {}
    );
  } finally {
    cubeDB.release();
  }
};

export const getReferenceDataDimensionPreview = async (
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension,
  tableName: string,
  lang: string
): Promise<ViewDTO> => {
  try {
    logger.debug('Passed validation preparing to send back the preview');

    const countQuery = `
      SELECT COUNT(DISTINCT ${tableName}."${dimension.factTableColumn}") AS total_rows
      FROM ${tableName}
    `;
    const countResult: { total_rows: number }[] = await cubeDB.query(countQuery);
    const totalRows = countResult[0].total_rows;

    const previewQuery = `
      SELECT DISTINCT ${tableName}."${dimension.factTableColumn}", reference_data_info.description
      FROM ${tableName}
      LEFT JOIN reference_data
        ON CAST(${tableName}."${dimension.factTableColumn}" AS VARCHAR)=reference_data.item_id
      JOIN reference_data_info
        ON reference_data.item_id=reference_data_info.item_id
        AND reference_data.category_key=reference_data_info.category_key
        AND reference_data.version_no=reference_data_info.version_no
      WHERE reference_data_info.lang='${lang.toLowerCase()}'
      LIMIT ${sampleSize}
    `;

    const previewResult = await cubeDB.query(previewQuery);
    const tableHeaders = Object.keys(previewResult[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = previewResult.map((row: any) => Object.values(row));

    const currentDataset = await DatasetRepository.getById(dataset.id, {
      dimensions: { metadata: true },
      revisions: { dataTable: true }
    });

    const headers: CSVHeader[] = tableHeaders.map((header, index) => {
      return {
        index,
        name: header,
        source_type: FactTableColumnType.Unknown
      };
    });
    const pageInfo = {
      total_records: totalRows,
      start_record: 1,
      end_record: dataArray.length
    };
    const pageSize = dataArray.length < sampleSize ? dataArray.length : sampleSize;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  } catch (error) {
    logger.error(`Something went wrong trying to generate the preview of the lookup table with error: ${error}`);
    throw error;
  }
};
