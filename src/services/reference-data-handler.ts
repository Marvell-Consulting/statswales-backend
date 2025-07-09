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
import { PoolClient, QueryResult } from 'pg';
import { getCubeDB } from '../db/cube-db';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { createReferenceDataTablesInCube, loadReferenceDataFromCSV } from './cube-handler';

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

async function copyAllReferenceDataIntoTable(connection: PoolClient): Promise<void> {
  logger.debug('Copying all reference data to the reference_data table.');
  await connection.query(`INSERT INTO reference_data (SELECT * FROM reference_data_all);`);
}

async function validateUnknownReferenceDataItems(
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension
): Promise<ViewErrDTO | undefined> {
  const nonMatchedRows = await connection.query(`
              SELECT fact_table."${dimension.factTableColumn}", reference_data.item_id FROM fact_table
              LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
              WHERE item_id IS NULL;
    `);
  if (nonMatchedRows.rows.length > 0) {
    logger.error('The user has unknown items in their reference data column');
    const nonMatchingDataTableValues = await connection.query(`
              SELECT DISTINCT fact_table."${dimension.factTableColumn}", reference_data.item_id FROM fact_table
              LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
              WHERE reference_data.item_id IS NULL;
        `);
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.unknown_reference_data_items', {
      totalNonMatching: nonMatchedRows.rows.length,
      nonMatchingDataTableValues: nonMatchingDataTableValues.rows.map((row) => Object.values(row)[0]),
      mismatch: true
    });
  }
  return undefined;
}

async function validateAllItemsAreInCategory(
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension,
  referenceDataType: ReferenceType,
  lang: string
): Promise<ViewErrDTO | undefined> {
  const nonMatchedRows = await connection.query(`
              SELECT fact_table."${dimension.factTableColumn}", reference_data.item_id, reference_data.category_key FROM fact_table
              LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
              JOIN category_keys ON reference_data.category_key=category_keys.category_key
              JOIN categories ON categories.category=category_keys.category
              WHERE categories.category!='${referenceDataType}';
    `);
  if (nonMatchedRows.rows.length > 0) {
    logger.error('The user has unknown items in their reference data column');
    const nonMatchingDataTableValues = await connection.query(`
            SELECT fact_table."${dimension.factTableColumn}", first(reference_data.category_key), first(categories.category), first(category_info.description) FROM fact_table
            LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
            JOIN category_keys ON reference_data.category_key=category_keys.category_key
            JOIN categories ON categories.category=category_keys.category JOIN category_info ON categories.category=category_info.category AND lang='${lang.toLowerCase()}'
            WHERE categories.category!='${referenceDataType}' GROUP BY fact_table."${dimension.factTableColumn}", item_id;
        `);
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.items_not_in_category', {
      totalNonMatching: nonMatchingDataTableValues.rows.length,
      nonMatchingDataTableValues: nonMatchingDataTableValues.rows.map((row) => Object.values(row)[0]),
      mismatch: true
    });
  }
  return undefined;
}

async function validateAllItemsAreInOneCategory(
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension
): Promise<ViewErrDTO | string> {
  const categoriesPresent: QueryResult<{ category: string }> = await connection.query(`
            SELECT DISTINCT categories.category as category FROM fact_table
            LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
            JOIN category_keys ON reference_data.category_key=category_keys.category_key
            JOIN categories ON categories.category=category_keys.category;
    `);
  if (categoriesPresent.rows.length > 1) {
    logger.error('The user has more than one type of category in reference data column');
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.to_many_categories_present', {
      totalNonMatching: categoriesPresent.rows.length,
      nonMatchingDataTableValues: categoriesPresent.rows.map((row) => Object.values(row)[0])
    });
  }
  if (categoriesPresent.rows.length === 0) {
    logger.error('There users column can not be matched to anything in the reference data');
    return viewErrorGenerators(
      400,
      dataset.id,
      'patch',
      'errors.dimension_validation.no_reference_data_categories_present',
      {}
    );
  }
  return categoriesPresent.rows[0].category;
}

export const validateReferenceData = async (
  dataset: Dataset,
  dimension: Dimension,
  referenceDataType: ReferenceType | undefined,
  lang: string
): Promise<ViewDTO | ViewErrDTO> => {
  const revision = dataset.draftRevision!;
  const connection = await getCubeDB().connect();
  try {
    await connection.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }
  try {
    // Load reference data in to cube
    await createReferenceDataTablesInCube(connection);
    await loadReferenceDataFromCSV(connection);
    await copyAllReferenceDataIntoTable(connection);
  } catch (err) {
    connection.release();
    logger.error(err, `Something went wrong trying to load the reference data into the cube`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.reference_data_loading_failed', {});
  }

  let confirmedReferenceDataCategory = referenceDataType?.toString();

  try {
    logger.debug(`Validating reference data`);
    const itemsNotPresentInReferenceData = await validateUnknownReferenceDataItems(connection, dataset, dimension);
    if (itemsNotPresentInReferenceData) {
      connection.release();
      return itemsNotPresentInReferenceData;
    }
    if (referenceDataType) {
      const itemsOutsideOfCategory = await validateAllItemsAreInCategory(
        connection,
        dataset,
        dimension,
        referenceDataType,
        lang
      );
      if (itemsOutsideOfCategory) {
        connection.release();
        return itemsOutsideOfCategory;
      }
    } else {
      const referenceDataCategory = await validateAllItemsAreInOneCategory(connection, dataset, dimension);
      if ((referenceDataCategory as ViewErrDTO).errors) {
        connection.release();
        return referenceDataCategory as ViewErrDTO;
      }
      confirmedReferenceDataCategory = referenceDataCategory as string;
    }
  } catch (error) {
    connection.release();
    logger.error(error, `Something went wrong trying to validate reference data`);
    return viewErrorGenerators(
      500,
      dataset.id,
      'patch',
      'errors.dimension_validation.reference_data_validation_failed.',
      {}
    );
  }

  const categoriesPresent: QueryResult<{ category_keys: string }> =
    await connection.query(`SELECT DISTINCT category_keys.category_key FROM fact_table
        LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
        JOIN category_keys ON reference_data.category_key=category_keys.category_key
        JOIN categories ON categories.category=category_keys.category
        WHERE categories.category='${confirmedReferenceDataCategory}';
    `);

  logger.debug(`Column passed reference data checks. Setting up dimension.`);
  await setupDimension(
    dimension,
    categoriesPresent.rows.map((row) => Object.values(row)[0])
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
    const allMatches = await connection.query(allMatchesQuery);
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
    const dimensionTable = await connection.query(previewQuery);
    const tableHeaders = Object.keys(dimensionTable.rows[0]);
    const dataArray = dimensionTable.rows.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id, { dimensions: { metadata: true } });
    const headers: CSVHeader[] = tableHeaders.map((header, index) => {
      return {
        index,
        name: header,
        source_type: FactTableColumnType.Unknown
      };
    });
    const pageInfo = {
      total_records: allMatches.rows.length,
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
    connection.release();
  }
};

export const getReferenceDataDimensionPreview = async (
  dataset: Dataset,
  dimension: Dimension,
  connection: PoolClient,
  tableName: string,
  lang: string
): Promise<ViewDTO> => {
  try {
    logger.debug('Passed validation preparing to send back the preview');

    const countQuery = `
            SELECT COUNT(DISTINCT ${tableName}."${dimension.factTableColumn}") AS total_rows
            FROM ${tableName}
        `;
    const countResult: QueryResult<{ total_rows: number }> = await connection.query(countQuery);
    const totalRows = countResult.rows[0].total_rows;

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

    const previewResult = await connection.query(previewQuery);
    const tableHeaders = Object.keys(previewResult.rows[0]);
    const dataArray = previewResult.rows.map((row) => Object.values(row));

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
