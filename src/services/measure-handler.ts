import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';
import { join } from 'lodash';

import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { columnIdentification, convertFactTableToLookupTable, lookForJoinColumn } from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { viewErrorGenerator } from '../utils/view-error-generator';
import { DataValueFormat } from '../enums/data-value-format';
import { logger } from '../utils/logger';
import { Measure } from '../entities/dataset/measure';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataTableDto } from '../dtos/data-table-dto';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';

import { createFactTableQuery } from './cube-handler';
import { DataLakeService } from './datalake';

async function cleanUpMeasure(measure: Measure) {
    if (!measure.lookupTable) return;
    logger.info(`Cleaning up previous lookup table`);
    try {
        const dataLakeService = new DataLakeService();
        await dataLakeService.deleteFile(measure.lookupTable.filename, measure.dataset.id);
    } catch (err) {
        logger.warn(`Something went wrong trying to remove previously uploaded lookup table with error: ${err}`);
    }

    try {
        const lookupTableId = measure.lookupTable.id;
        measure.measureInfo = null;
        measure.joinColumn = null;
        measure.extractor = null;
        measure.lookupTable = null;
        await measure.save();
        logger.debug(`Removing orphaned measure lookup table`);
        const oldLookupTable = await LookupTable.findOneBy({ id: lookupTableId });
        await oldLookupTable?.remove();
    } catch (err) {
        logger.error(
            `Something has gone wrong trying to unlink the previous lookup table from the measure with the following error: ${err}`
        );
        throw err;
    }
}

function createExtractor(
    protoLookupTable: DataTable,
    tableMatcher?: MeasureLookupPatchDTO
): MeasureLookupTableExtractor {
    if (tableMatcher?.description_columns) {
        logger.debug('Using user supplied table matcher to match columns');
        return {
            sortColumn: tableMatcher?.sort_column,
            formatColumn: tableMatcher?.format_column,
            measureTypeColumn: tableMatcher?.measure_type_column,
            descriptionColumns: tableMatcher.description_columns.map(
                (desc) =>
                    protoLookupTable.dataTableDescriptions
                        .filter((info) => info.columnName === desc)
                        .map((info) => columnIdentification(info))[0]
            ),
            notesColumns: tableMatcher.notes_columns?.map(
                (desc) =>
                    protoLookupTable.dataTableDescriptions
                        .filter((info) => info.columnName === desc)
                        .map((info) => columnIdentification(info))[0]
            )
        };
    } else {
        logger.debug('Detecting column types from column names');
        let notesColumns: ColumnDescriptor[] | undefined;
        if (protoLookupTable.dataTableDescriptions.filter((info) => info.columnName.toLowerCase().startsWith('note')))
            notesColumns = protoLookupTable.dataTableDescriptions
                .filter((info) => info.columnName.toLowerCase().startsWith('note'))
                .map((info) => columnIdentification(info));
        return {
            sortColumn: protoLookupTable.dataTableDescriptions.find((info) => info.columnName.toLowerCase().startsWith('sort'))
                ?.columnName,
            formatColumn: protoLookupTable.dataTableDescriptions.find(
                (info) =>
                    info.columnName.toLowerCase().indexOf('format') > -1 ||
                    info.columnName.toLowerCase().indexOf('decimal') > -1
            )?.columnName,
            measureTypeColumn: protoLookupTable.dataTableDescriptions.find(
                (info) => info.columnName.toLowerCase().indexOf('type') > -1
            )?.columnName,
            descriptionColumns: protoLookupTable.dataTableDescriptions
                .filter((info) => info.columnName.toLowerCase().startsWith('description'))
                .map((info) => columnIdentification(info)),
            notesColumns
        };
    }
}

async function setupMeasure(
    dataset: Dataset,
    lookupTable: LookupTable,
    protoLookupTable: DataTable,
    confirmedJoinColumn: string,
    tableMatcher?: MeasureLookupPatchDTO
) {
    // Clean up previously uploaded dimensions
    if (dataset.measure.lookupTable) await cleanUpMeasure(dataset.measure);
    lookupTable.isStatsWales2Format = !protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith('lang')
    );
    const updateMeasure = await Measure.findOneByOrFail({ id: dataset.measure.id });
    updateMeasure.joinColumn = confirmedJoinColumn;
    updateMeasure.lookupTable = lookupTable;
    updateMeasure.extractor = createExtractor(protoLookupTable, tableMatcher);
    logger.debug('Saving the lookup table');
    await lookupTable.save();
    logger.debug('Saving the dimension');
    updateMeasure.lookupTable = lookupTable;
    await updateMeasure.save();
}

async function rowMatcher(
    quack: Database,
    measure: Measure,
    datasetId: string,
    lookupTableName: string,
    factTableName: string,
    confirmedJoinColumn: string
): Promise<ViewErrDTO | undefined> {
    try {
        const nonMatchedRowQuery = `SELECT line_number, fact_table_column, ${lookupTableName}."${confirmedJoinColumn}" as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${measure.factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
            WHERE ${lookupTableName}."${confirmedJoinColumn}" IS NULL;`;
        logger.debug(`Running row matching query: ${nonMatchedRowQuery}`);
        const nonMatchedRows = await quack.all(nonMatchedRowQuery);
        const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
        if (nonMatchedRows.length === rows[0].total_rows) {
            logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT ${measure.factTableColumn} FROM ${factTableName};`
            );
            return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
                totalNonMatching: rows[0].total_rows,
                nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
            });
        }
        if (nonMatchedRows.length > 0) {
            logger.error(`Seems some of the rows didn't match.`);
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT fact_table_column FROM (SELECT "${measure.factTableColumn}" as fact_table_column
                FROM ${factTableName}) as fact_table
                LEFT JOIN ${lookupTableName} ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
                where ${lookupTableName}."${confirmedJoinColumn}" IS NULL;`
            );
            logger.error(
                `The user supplied an incorrect or incomplete lookup table and ${nonMatchedRows.length} rows didn't match`
            );
            return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
                totalNonMatching: nonMatchedRows.length,
                nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
            });
        }
    } catch (error) {
        logger.error(
            `Something went wrong, most likely an incorrect join column name, while trying to validate the lookup table with error: ${error}`
        );
        const nonMatchedRows = await quack.all(`SELECT COUNT(*) AS total_rows FROM ${factTableName};`);
        const nonMatchedValues = await quack.all(`SELECT DISTINCT ${measure.factTableColumn} FROM ${factTableName};`);
        return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
            totalNonMatching: nonMatchedRows[0].total_rows,
            nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
        });
    }
    logger.debug('The measure lookup table passed row matching.');
    return undefined;
}

async function validateTableContent(
    quack: Database,
    datasetId: string,
    lookupTableName: string,
    extractor: MeasureLookupTableExtractor
): Promise<ViewErrDTO | undefined> {
    const unmatchedFormats: string[] = [];
    if (extractor.formatColumn && extractor.formatColumn.toLowerCase().indexOf('format') > -1) {
        logger.debug('Formats column is present.  Validating all formats present are valid.');
        const formats = await quack.all(
            `SELECT DISTINCT "${extractor.formatColumn}" as formats FROM ${lookupTableName};`
        );
        for (const format of Object.values(formats.map((format) => format.formats))) {
            if (Object.values(DataValueFormat).indexOf(format) === -1) unmatchedFormats.push(format);
        }
    } else if (extractor.formatColumn && extractor.formatColumn.toLowerCase().indexOf('decimal') !== -1) {
        logger.debug('Decimal column is present.  Validating contains only 1 or 0.');
        const formats = await quack.all(
            `SELECT DISTINCT "${extractor.formatColumn}" as formats FROM ${lookupTableName};`
        );
        for (const format of Object.values(formats.map((format) => format.formats))) {
            if (format < 0 && format > 1) unmatchedFormats.push(format);
        }
    }
    if (unmatchedFormats.length > 0) {
        logger.debug(
            `Found invalid formats while validating format column.  Formats found: ${JSON.stringify(unmatchedFormats)}`
        );
        return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
            totalNonMatching: unmatchedFormats.length,
            nonMatchingValues: unmatchedFormats
        });
    }
    logger.debug('Validating column contents complete.');
    return undefined;
}

export const validateMeasureLookupTable = async (
    protoLookupTable: DataTable,
    factTable: DataTable,
    dataset: Dataset,
    buffer: Buffer,
    tableMatcher?: MeasureLookupPatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
    const lookupTable = convertFactTableToLookupTable(protoLookupTable, undefined, dataset?.measure);
    const factTableName = 'fact_table';
    const lookupTableName = 'preview_lookup';
    const measure = dataset.measure;
    const quack = await Database.create(':memory:');
    const lookupTableTmpFile = tmp.tmpNameSync({ postfix: `.${lookupTable.fileType}` });
    try {
        fs.writeFileSync(lookupTableTmpFile, buffer);
        const factTableTmpFile = await getFileImportAndSaveToDisk(dataset, factTable);
        await loadFileIntoDatabase(quack, factTable, factTableTmpFile, factTableName);
        await loadFileIntoDatabase(quack, lookupTable, lookupTableTmpFile, lookupTableName);
        fs.unlinkSync(lookupTableTmpFile);
        fs.unlinkSync(factTableTmpFile);
    } catch (err) {
        logger.error(`Something went wrong trying to load data in to DuckDB with the following error: ${err}`);
        throw err;
    }

    let confirmedJoinColumn: string | undefined;
    try {
        confirmedJoinColumn = lookForJoinColumn(protoLookupTable, measure.factTableColumn, tableMatcher);
    } catch (err) {
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.no_join_column', {});
    }

    if (!confirmedJoinColumn) {
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.no_join_column', {});
    }

    const rowMatchingErrors = await rowMatcher(
        quack,
        measure,
        dataset.id,
        lookupTableName,
        factTableName,
        confirmedJoinColumn
    );
    if (rowMatchingErrors) return rowMatchingErrors;
    const extractor = createExtractor(protoLookupTable, tableMatcher);
    const tableValidationErrors = await validateTableContent(quack, dataset.id, lookupTableName, extractor);
    if (tableValidationErrors) return tableValidationErrors;

    await setupMeasure(dataset, lookupTable, protoLookupTable, confirmedJoinColumn, tableMatcher);

    try {
        const dimensionTable = await quack.all(`SELECT * FROM ${lookupTableName};`);
        await quack.close();
        const tableHeaders = Object.keys(dimensionTable[0]);
        const dataArray = dimensionTable.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            let sourceType: FactTableColumnType;
            if (tableHeaders[i] === 'int_line_number') sourceType = FactTableColumnType.LineNumber;
            else
                sourceType = FactTableColumnType.Unknown;
            headers.push({
                index: i - 1,
                name: tableHeaders[i],
                source_type: sourceType
            });
        }
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            fact_table: DataTableDto.fromDataTable(currentImport),
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

const sampleSize = 5;

async function getMeasurePreviewWithoutExtractor(
    dataset: Dataset,
    measure: Measure,
    factTable: DataTable,
    quack: Database,
    tableName: string
): Promise<ViewDTO> {
    const preview = await quack.all(
        `SELECT DISTINCT "${measure.factTableColumn}" FROM ${tableName} ORDER BY "${measure.factTableColumn}" ASC LIMIT ${sampleSize};`
    );
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
    const headers: CSVHeader[] = [];
    for (let i = 0; i < tableHeaders.length; i++) {
        headers.push({
            index: i,
            name: tableHeaders[i],
            source_type: FactTableColumnType.Unknown
        });
    }
    return {
        dataset: DatasetDTO.fromDataset(currentDataset),
        fact_table: DataTableDto.fromDataTable(currentImport),
        current_page: 1,
        page_info: {
            total_records: preview.length,
            start_record: 1,
            end_record: preview.length
        },
        page_size: preview.length < sampleSize ? preview.length : sampleSize,
        total_pages: 1,
        headers,
        data: dataArray
    };
}

async function getMeasurePreviewWithExtractor(
    dataset: Dataset,
    measure: Measure,
    factTable: DataTable,
    quack: Database,
    tableName: string
) {
    if (!measure.lookupTable) {
        throw new Error(`Lookup table does does not exist on measure ${measure.id}`);
    }
    logger.debug(`Generating lookup table preview for measure ${measure.id}`);
    const lookupTmpFile = await getFileImportAndSaveToDisk(dataset, measure.lookupTable);
    const lookupTableName = `lookup_table`;
    await loadFileIntoDatabase(quack, measure.lookupTable, lookupTmpFile, lookupTableName);
    const sortColumn = (measure.extractor as LookupTableExtractor).sortColumn || measure.joinColumn;
    const query = `SELECT * FROM ${lookupTableName} ORDER BY ${sortColumn} LIMIT ${sampleSize};`;
    logger.debug(`Querying the cube to get the preview using query ${query}`);
    const measureTable = await quack.all(query);
    const tableHeaders = Object.keys(measureTable[0]);
    const dataArray = measureTable.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
    const headers: CSVHeader[] = tableHeaders.map((name, idx) => ({
        name,
        index: idx,
        source_type: FactTableColumnType.Unknown
    }));
    return {
        dataset: DatasetDTO.fromDataset(currentDataset),
        fact_table: DataTableDto.fromDataTable(currentImport),
        current_page: 1,
        page_info: {
            total_records: measureTable.length,
            start_record: 1,
            end_record: measureTable.length < sampleSize ? measureTable.length : sampleSize
        },
        page_size: measureTable.length < sampleSize ? measureTable.length : sampleSize,
        total_pages: 1,
        headers,
        data: dataArray
    };
}

export const getMeasurePreview = async (dataset: Dataset, factTable: DataTable) => {
    logger.debug(`Getting measure preview for ${dataset.measure.id}`);
    const tableName = 'fact_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.tmpNameSync({ postfix: `.${factTable.fileType}` });
    const measure = dataset.measure;
    if (!measure) {
        throw new Error('No measure present on the dataset.');
    }
    // extract the data from the fact table
    try {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(factTable.filename, dataset.id);
        fs.writeFileSync(tempFile, fileBuffer);
        const createTableQuery = await createFactTableQuery(tableName, tempFile, factTable.fileType, quack);
        await quack.exec(createTableQuery);
    } catch (error) {
        logger.error(
            `Something went wrong trying to create ${tableName} in DuckDB.  Unable to do matching and validation`
        );
        await quack.close();
        fs.unlinkSync(tempFile);
        throw error;
    }
    let viewDto: ViewDTO;
    try {
        if (measure.measureInfo && measure.measureInfo.length > 0) {
            throw new Error(`Measure tables from measure info are not yet supported`);
        } else if (dataset.measure.extractor) {
            viewDto = await getMeasurePreviewWithExtractor(dataset, measure, factTable, quack, tableName);
        } else {
            logger.debug('Straight column preview');
            viewDto = await getMeasurePreviewWithoutExtractor(dataset, measure, factTable, quack, tableName);
        }
        await quack.close();
        fs.unlinkSync(tempFile);
        return viewDto;
    } catch (error) {
        logger.error(`Something went wrong trying to create measure preview with the following error: ${error}`);
        await quack.close();
        fs.unlinkSync(tempFile);
        throw error;
    }
};
