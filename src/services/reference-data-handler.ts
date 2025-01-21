import fs from 'fs';

import { Database } from 'duckdb-async';

import { FactTable } from '../entities/dataset/fact-table';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { logger } from '../utils/logger';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { viewErrorGenerator } from '../utils/view-error-generator';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';
import { ReferenceType } from '../enums/reference-type';
import { DimensionType } from '../enums/dimension-type';

// eslint-disable-next-line import/no-cycle
import { cleanUpDimension } from './dimension-processor';
import {
    cleanUpReferenceDataTables,
    loadCorrectReferenceDataIntoReferenceDataTable,
    loadReferenceDataIntoCube
} from './cube-handler';

async function setupDimension(dimension: Dimension, categories: string[]) {
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

async function copyAllReferenceDataIntoTable(quack: Database) {
    logger.debug('Copying all reference data to the reference_data table.');
    await quack.exec(`INSERT INTO reference_data (SELECT * FROM reference_data_all);`);
}

async function validateUnknownReferenceDataItems(quack: Database, dataset: Dataset, dimension: Dimension) {
    const nonMatchedRows = await quack.all(`
              SELECT fact_table."${dimension.factTableColumn}", reference_data.item_id FROM fact_table
              LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
              WHERE item_id IS NULL;
    `);
    if (nonMatchedRows.length > 0) {
        logger.error('The user has unknown items in their reference data column');
        const nonMatchedValues = await quack.all(`
              SELECT DISTINCT fact_table."${dimension.factTableColumn}", reference_data.item_id FROM fact_table
              LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
              WHERE reference_data.item_id IS NULL;
        `);
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
            totalNonMatching: nonMatchedRows.length,
            nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
        });
    }
    return undefined;
}

async function validateAllItemsAreInCategory(
    quack: Database,
    dataset: Dataset,
    dimension: Dimension,
    referenceDataType: ReferenceType,
    lang: string
) {
    const nonMatchedRows = await quack.all(`
              SELECT fact_table."${dimension.factTableColumn}", reference_data.item_id, reference_data.category_key FROM fact_table
              LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
              JOIN category_keys ON reference_data.category_key=category_keys.category_key
              JOIN categories ON categories.category=category_keys.category
              WHERE categories.category!='${referenceDataType}';
    `);
    if (nonMatchedRows.length > 0) {
        logger.error('The user has unknown items in their reference data column');
        const nonMatchedValues = await quack.all(`
            SELECT fact_table."${dimension.factTableColumn}", first(reference_data.category_key), first(categories.category), first(category_info.description) FROM fact_table
            LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
            JOIN category_keys ON reference_data.category_key=category_keys.category_key
            JOIN categories ON categories.category=category_keys.category JOIN category_info ON categories.category=category_info.category AND lang='${lang.toLowerCase()}'
            WHERE categories.category!='${referenceDataType}' GROUP BY fact_table."${dimension.factTableColumn}", item_id;
        `);
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
            totalNonMatching: nonMatchedValues.length,
            nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
        });
    }
    return undefined;
}

async function validateAllItemsAreInOneCategory(
    quack: Database,
    dataset: Dataset,
    dimension: Dimension
): Promise<ViewErrDTO | string> {
    const categoriesPresent = await quack.all(`
            SELECT DISTINCT categories.category FROM fact_table
            LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
            JOIN category_keys ON reference_data.category_key=category_keys.category_key
            JOIN categories ON categories.category=category_keys.category;
    `);
    if (categoriesPresent.length > 1) {
        logger.error('The user has more than one type of category in reference data column');
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.to_many_categories_present', {
            totalNonMatching: categoriesPresent.length,
            nonMatchingValues: categoriesPresent.map((row) => Object.values(row)[0])
        });
    }
    if (categoriesPresent.length === 0) {
        logger.error('There users column can not be matched to anything in the reference data');
        return viewErrorGenerator(
            400,
            dataset.id,
            'patch',
            'errors.dimensionValidation.no_reference_data_categories_present',
            {}
        );
    }
    return categoriesPresent[0].category;
}

export const validateReferenceData = async (
    factTable: FactTable,
    dataset: Dataset,
    dimension: Dimension,
    referenceDataType: ReferenceType | undefined,
    lang: string
): Promise<ViewDTO | ViewErrDTO> => {
    const factTableName = 'fact_table';
    const quack = await Database.create(':memory:');
    try {
        // Load reference data in to cube
        await loadReferenceDataIntoCube(quack);
        await copyAllReferenceDataIntoTable(quack);
        const factTableTmpFile = await getFileImportAndSaveToDisk(dataset, factTable);
        logger.debug(`Loading fact table in to DuckDB`);
        await loadFileIntoDatabase(quack, factTable, factTableTmpFile, factTableName);
        fs.unlinkSync(factTableTmpFile);
    } catch (err) {
        await quack.close();
        logger.error(`Something went wrong trying to load data in to DuckDB with the following error: ${err}`);
        throw err;
    }

    let confirmedReferenceDataCategory = referenceDataType?.toString();

    try {
        logger.debug(`Validating reference data`);
        const itemsNotPresentInReferenceData = await validateUnknownReferenceDataItems(quack, dataset, dimension);
        if (itemsNotPresentInReferenceData) {
            await quack.close();
            return itemsNotPresentInReferenceData;
        }
        if (referenceDataType) {
            const itemsOutsideOfCategory = await validateAllItemsAreInCategory(
                quack,
                dataset,
                dimension,
                referenceDataType,
                lang
            );
            if (itemsOutsideOfCategory) {
                await quack.close();
                return itemsOutsideOfCategory;
            }
        } else {
            const referenceDataCategory = await validateAllItemsAreInOneCategory(quack, dataset, dimension);
            if ((referenceDataCategory as ViewErrDTO).errors) {
                await quack.close();
                return referenceDataCategory as ViewErrDTO;
            }
            confirmedReferenceDataCategory = referenceDataCategory as string;
        }
    } catch (error) {
        await quack.close();
        logger.error(`Something went wrong trying to validate reference data with the following error: ${error}`);
        throw new Error(`Something went wrong trying to validate reference data with the following error: ${error}`);
    }

    const categoriesPresent = await quack.all(`SELECT DISTINCT category_keys.category_key FROM fact_table
        LEFT JOIN reference_data on reference_data.item_id=CAST(fact_table."${dimension.factTableColumn}" AS VARCHAR)
        JOIN category_keys ON reference_data.category_key=category_keys.category_key
        JOIN categories ON categories.category=category_keys.category
        WHERE categories.category='${confirmedReferenceDataCategory}';
    `);

    logger.debug(`Column passed reference data checks.  Setting up dimension.`);
    await setupDimension(
        dimension,
        categoriesPresent.map((row) => Object.values(row)[0])
    );

    try {
        logger.debug('Passed validation preparing to send back the preview');
        const previewQuery = `
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
        logger.debug(`Preview query = ${previewQuery}`);
        const dimensionTable = await quack.all(previewQuery);
        await quack.close();
        const tableHeaders = Object.keys(dimensionTable[0]);
        const dataArray = dimensionTable.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await FactTable.findOneByOrFail({ id: factTable.id });
        const headers: CSVHeader[] = tableHeaders.map((header, index) => {
            return {
                index,
                name: header,
                source_type: FactTableColumnType.Unknown
            };
        });
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            fact_table: FactTableDTO.fromFactTable(currentImport),
            current_page: 1,
            page_info: {
                total_records: 1,
                start_record: 1,
                end_record: 10
            },
            page_size: 10,
            total_pages: 1,
            headers,
            data: dataArray
        };
    } catch (error) {
        logger.error(`Something went wrong trying to generate the preview of the lookup table with error: ${error}`);
        throw error;
    }
};

export const getReferenceDataDimensionPreview = async (
    dataset: Dataset,
    dimension: Dimension,
    factTable: FactTable,
    quack: Database,
    tableName: string,
    lang: string
) => {
    logger.debug('Loading correct reference data into empty Cube');
    await loadReferenceDataIntoCube(quack);
    await loadCorrectReferenceDataIntoReferenceDataTable(quack, dimension);
    await cleanUpReferenceDataTables(quack);
    try {
        logger.debug('Passed validation preparing to send back the preview');
        const previewQuery = `SELECT DISTINCT ${tableName}."${dimension.factTableColumn}", reference_data_info.description
            FROM ${tableName}
            LEFT JOIN reference_data
                ON CAST(${tableName}."${dimension.factTableColumn}" AS VARCHAR)=reference_data.item_id
            JOIN reference_data_info
                ON reference_data.item_id=reference_data_info.item_id
                AND reference_data.category_key=reference_data_info.category_key
                AND reference_data.version_no=reference_data_info.version_no
            WHERE reference_data_info.lang='${lang.toLowerCase()}';`;
        logger.debug(`Preview Query = ${previewQuery}`);
        const dimensionTable = await quack.all(previewQuery);
        logger.debug(`Query Result = ${JSON.stringify(dimensionTable, null, 2)}`);
        const tableHeaders = Object.keys(dimensionTable[0]);
        const dataArray = dimensionTable.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await FactTable.findOneByOrFail({ id: factTable.id });
        const headers: CSVHeader[] = tableHeaders.map((header, index) => {
            return {
                index,
                name: header,
                source_type: FactTableColumnType.Unknown
            };
        });
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            fact_table: FactTableDTO.fromFactTable(currentImport),
            current_page: 1,
            page_info: {
                total_records: 1,
                start_record: 1,
                end_record: 10
            },
            page_size: 10,
            total_pages: 1,
            headers,
            data: dataArray
        };
    } catch (error) {
        logger.error(`Something went wrong trying to generate the preview of the lookup table with error: ${error}`);
        throw error;
    }
};
