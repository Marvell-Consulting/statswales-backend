import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';
import { NextFunction, Request, Response } from 'express';

import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTable } from '../entities/dataset/fact-table';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';
import { DataLakeService } from '../services/datalake';
import { columnIdentification, convertFactTableToLookupTable } from '../utils/lookup-table-utils';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { viewErrorGenerator } from '../utils/view-error-generator';
import { Measure } from '../entities/dataset/measure';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { DataValueFormat } from '../enums/data-value-format';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';

import { uploadCSV } from './csv-processor';
import { createFactTableQuery } from './cube-handler';

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
    protoLookupTable: FactTable,
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
                    protoLookupTable.factTableInfo
                        .filter((info) => info.columnName === desc)
                        .map((info) => columnIdentification(info))[0]
            ),
            notesColumns: tableMatcher.notes_columns?.map(
                (desc) =>
                    protoLookupTable.factTableInfo
                        .filter((info) => info.columnName === desc)
                        .map((info) => columnIdentification(info))[0]
            )
        };
    } else {
        logger.debug('Detecting column types from column names');
        let notesColumns: ColumnDescriptor[] | undefined;
        if (protoLookupTable.factTableInfo.filter((info) => info.columnName.toLowerCase().startsWith('note')))
            notesColumns = protoLookupTable.factTableInfo
                .filter((info) => info.columnName.toLowerCase().startsWith('note'))
                .map((info) => columnIdentification(info));
        return {
            sortColumn: protoLookupTable.factTableInfo.find((info) => info.columnName.toLowerCase().startsWith('sort'))
                ?.columnName,
            formatColumn: protoLookupTable.factTableInfo.find(
                (info) =>
                    info.columnName.toLowerCase().startsWith('format') ||
                    info.columnName.toLowerCase().startsWith('decimal')
            )?.columnName,
            measureTypeColumn: protoLookupTable.factTableInfo.find(
                (info) =>
                    info.columnName.toLowerCase().indexOf('measure') > -1 &&
                    info.columnName.toLowerCase().indexOf('type') > -1
            )?.columnName,
            descriptionColumns: protoLookupTable.factTableInfo
                .filter((info) => info.columnName.toLowerCase().startsWith('description'))
                .map((info) => columnIdentification(info)),
            notesColumns
        };
    }
}

function lookForJoinColumn(protoLookupTable: FactTable, tableMatcher?: MeasureLookupPatchDTO): string {
    if (tableMatcher?.join_column) {
        return tableMatcher.join_column;
    } else {
        const possibleJoinColumns = protoLookupTable.factTableInfo.filter((info) => {
            if (info.columnName.toLowerCase().startsWith('decimal')) return false;
            if (info.columnName.toLowerCase().startsWith('format')) return false;
            if (info.columnName.toLowerCase().startsWith('description')) return false;
            if (info.columnName.toLowerCase().startsWith('sort')) return false;
            if (info.columnName.toLowerCase().startsWith('note')) return false;
            return true;
        });
        if (possibleJoinColumns.length > 1) {
            throw new Error('There are to many possible join columns.  Ask user for more information');
        }
        if (possibleJoinColumns.length === 0) {
            throw new Error('Could not find a column to join against the fact table.');
        }
        logger.debug(`Found the following join column ${JSON.stringify(possibleJoinColumns)}`);
        return possibleJoinColumns[0].columnName;
    }
}

async function setupMeasure(
    dataset: Dataset,
    lookupTable: LookupTable,
    protoLookupTable: FactTable,
    confirmedJoinColumn: string,
    tableMatcher?: MeasureLookupPatchDTO
) {
    // Clean up previously uploaded dimensions
    if (dataset.measure.lookupTable) await cleanUpMeasure(dataset.measure);
    lookupTable.isStatsWales2Format = !protoLookupTable.factTableInfo.find((info) =>
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
        const nonMatchedRows =
            await quack.all(`SELECT line_number, fact_table_column, ${lookupTableName}.${confirmedJoinColumn} as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${measure.factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
            WHERE lookup_table_column IS NULL;`);
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
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT fact_table_column FROM (SELECT "${measure.factTableColumn}" as fact_table_column FROM ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR) where lookup_table_column IS NULL;`
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
    if (extractor.formatColumn && extractor.formatColumn.toLowerCase().indexOf('format') !== -1) {
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
    if (extractor.measureTypeColumn) {
        logger.debug(
            `Measure type column (${extractor.measureTypeColumn}) is present, validating all type present are valid`
        );
        const unmatchedMeasureTypes: string[] = [];
        const measureTypes = await quack.all(
            `SELECT DISTINCT "${extractor.measureTypeColumn}" as formats FROM ${lookupTableName};`
        );
        for (const measureType of Object.values(measureTypes.map((measureType) => measureType.formats))) {
            if (Object.values(DataValueFormat).indexOf(measureType) === -1) unmatchedMeasureTypes.push(measureType);
        }
        if (unmatchedMeasureTypes.length > 0) {
            return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
                totalNonMatching: unmatchedMeasureTypes.length,
                nonMatchingValues: unmatchedMeasureTypes
            });
        }
    }
    logger.debug('Validating column contents complete.');
    return undefined;
}

export const validateMeasureLookupTable = async (
    protoLookupTable: FactTable,
    factTable: FactTable,
    dataset: Dataset,
    buffer: Buffer,
    tableMatcher?: MeasureLookupPatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
    const lookupTable = convertFactTableToLookupTable(protoLookupTable, undefined, dataset?.measure);
    const factTableName = 'fact_table';
    const lookupTableName = 'preview_lookup';
    const measure = dataset.measure;
    const quack = await Database.create(':memory:');
    const lookupTableTmpFile = tmp.fileSync({ postfix: `.${lookupTable.fileType}` });
    try {
        fs.writeFileSync(lookupTableTmpFile.name, buffer);
        const factTableTmpFile = await getFileImportAndSaveToDisk(dataset, factTable);
        await loadFileIntoDatabase(quack, factTable, factTableTmpFile, factTableName);
        await loadFileIntoDatabase(quack, lookupTable, lookupTableTmpFile, lookupTableName);
        lookupTableTmpFile.removeCallback();
        factTableTmpFile.removeCallback();
    } catch (err) {
        logger.error(`Something went wrong trying to load data in to DuckDB with the following error: ${err}`);
        throw err;
    }

    const confirmedJoinColumn: string = lookForJoinColumn(protoLookupTable, tableMatcher);

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
        const currentImport = await FactTable.findOneByOrFail({ id: factTable.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            let sourceType: FactTableColumnType;
            if (tableHeaders[i] === 'int_line_number') sourceType = FactTableColumnType.LineNumber;
            else
                sourceType =
                    factTable.factTableInfo.find((info) => info.columnName === tableHeaders[i])?.columnType ??
                    FactTableColumnType.Unknown;
            headers.push({
                index: i - 1,
                name: tableHeaders[i],
                source_type: sourceType
            });
        }
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

const sampleSize = 5;

async function getMeasurePreviewWithoutExtractor(
    dataset: Dataset,
    measure: Measure,
    factTable: FactTable,
    quack: Database,
    tableName: string
): Promise<ViewDTO> {
    const preview = await quack.all(
        `SELECT DISTINCT "${measure.factTableColumn}" FROM ${tableName} ORDER BY "${measure.factTableColumn}" ASC LIMIT ${sampleSize};`
    );
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await FactTable.findOneByOrFail({ id: factTable.id });
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
        fact_table: FactTableDTO.fromFactTable(currentImport),
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
    factTable: FactTable,
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
    const currentImport = await FactTable.findOneByOrFail({ id: factTable.id });
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
        fact_table: FactTableDTO.fromFactTable(currentImport),
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

export const getMeasurePreview = async (dataset: Dataset, factTable: FactTable) => {
    logger.debug(`Getting measure preview for ${dataset.measure.id}`);
    const tableName = 'fact_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: `.${factTable.fileType}` });
    const measure = dataset.measure;
    if (!measure) {
        throw new Error('No measure present on the dataset.');
    }
    // extract the data from the fact table
    try {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(factTable.filename, dataset.id);
        fs.writeFileSync(tempFile.name, fileBuffer);
        const createTableQuery = await createFactTableQuery(tableName, tempFile.name, factTable.fileType, quack);
        await quack.exec(createTableQuery);
    } catch (error) {
        logger.error(
            `Something went wrong trying to create ${tableName} in DuckDB.  Unable to do matching and validation`
        );
        await quack.close();
        tempFile.removeCallback();
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
        tempFile.removeCallback();
        return viewDto;
    } catch (error) {
        logger.error(`Something went wrong trying to create measure preview with the following error: ${error}`);
        await quack.close();
        tempFile.removeCallback();
        throw error;
    }
};

export const resetMeasure = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const measure = dataset.measure;
    if (!measure) {
        next(new NotFoundException('errors.measure_missing'));
        return;
    }
    logger.debug('Resetting measure by removing extractor, lookup table, info and join column');
    measure.extractor = null;
    if (measure.lookup) {
        const measureLookupFilename = measure.lookup.filename;
        const lookupTable: LookupTable = measure.lookupTable;
        await lookupTable.remove();
        measure.lookupTable = null;
        logger.debug(`Removing file ${dataset.id}/${measureLookupFilename} from data lake`);
        const datalakeService = new DataLakeService();
        await datalakeService.deleteFile(measureLookupFilename, dataset.id);
    }
    if (measure.measureInfo) {
        logger.debug('Removing all measure info');
        for (const info of measure.measureInfo) {
            await info.remove();
        }
    }
    measure.joinColumn = null;
    logger.debug('Saving measure and returning dataset');
    await measure.save();
    const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
    res.status(200);
    const dto = DatasetDTO.fromDataset(updateDataset);
    res.json(dto);
};

export const attachLookupTableToMeasure = async (req: Request, res: Response, next: NextFunction) => {
    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }
    const dataset: Dataset = res.locals.dataset;

    // Replace calls that require this to calls that get a single factTable for all revisions to "present"
    const factTable = getLatestRevision(dataset)?.factTables[0];
    if (!factTable) {
        next(new NotFoundException('errors.fact_table_invalid'));
        return;
    }
    let fileImport: FactTable;
    try {
        fileImport = await uploadCSV(req.file.buffer, req.file?.mimetype, req.file?.originalname, res.locals.datasetId);
    } catch (err) {
        logger.error(`An error occurred trying to upload the file: ${err}`);
        next(new UnknownException('errors.upload_error'));
        return;
    }

    const tableMatcher = req.body as MeasureLookupPatchDTO;

    try {
        const result = await validateMeasureLookupTable(fileImport, factTable, dataset, req.file.buffer, tableMatcher);
        if ((result as ViewErrDTO).status) {
            const error = result as ViewErrDTO;
            res.status(error.status);
            res.json(result);
            return;
        }
        res.status(200);
        res.json(result);
    } catch (err) {
        logger.error(`An error occurred trying to handle measure lookup table with error: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }
};

export const getPreviewOfMeasure = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const factTable = getLatestRevision(dataset)?.factTables[0];
    if (!dataset.measure) {
        next(new NotFoundException('errors.measure_invalid'));
        return;
    }
    if (!factTable) {
        next(new NotFoundException('errors.fact_table_invalid'));
        return;
    }
    try {
        const preview = await getMeasurePreview(dataset, factTable);
        res.status(200);
        res.json(preview);
    } catch (err) {
        logger.error(`Something went wrong trying to get a preview of the dimension with the following error: ${err}`);
        res.status(500);
        res.json({ message: 'Something went wrong trying to generate a preview of the dimension' });
    }
};
