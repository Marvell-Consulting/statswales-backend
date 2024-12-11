import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';
import { t } from 'i18next';
import { View } from 'typeorm';

import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { FactTable } from '../entities/dataset/fact-table';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { logger } from '../utils/logger';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { FactTableInfo } from '../entities/dataset/fact-table-info';
import { Measure } from '../entities/dataset/measure';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { DataLakeService } from '../services/datalake';
import { FileType } from '../enums/file-type';
import { Locale } from '../enums/locale';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';

import { DateExtractor, DateReferenceDataItem, dateDimensionReferenceTableCreator } from './time-matching';

const createDateDimensionTable = `CREATE TABLE date_dimension (date_code VARCHAR, description VARCHAR, start_date datetime, end_date datetime, date_type varchar);`;
const sampleSize = 5;

export interface ValidatedSourceAssignment {
    dataValues: SourceAssignmentDTO | null;
    noteCodes: SourceAssignmentDTO | null;
    measure: SourceAssignmentDTO | null;
    dimensions: SourceAssignmentDTO[];
    ignore: SourceAssignmentDTO[];
}

export const validateSourceAssignment = (
    fileImport: FactTable,
    sourceAssignment: SourceAssignmentDTO[]
): ValidatedSourceAssignment => {
    let dataValues: SourceAssignmentDTO | null = null;
    let noteCodes: SourceAssignmentDTO | null = null;
    let measure: SourceAssignmentDTO | null = null;
    const dimensions: SourceAssignmentDTO[] = [];
    const dateDimensions: SourceAssignmentDTO[] = [];
    const ignore: SourceAssignmentDTO[] = [];

    sourceAssignment.map((sourceInfo) => {
        if (!fileImport.factTableInfo?.find((info: FactTableInfo) => info.columnName === sourceInfo.column_name)) {
            throw new Error(`Source with id ${sourceInfo.column_name} not found`);
        }

        switch (sourceInfo.column_type) {
            case FactTableColumnType.DataValues:
                if (dataValues) {
                    throw new SourceAssignmentException('errors.too_many_data_values');
                }
                dataValues = sourceInfo;
                break;
            case FactTableColumnType.Measure:
                if (measure) {
                    throw new SourceAssignmentException('errors.too_many_measure');
                }
                measure = sourceInfo;
                break;
            case FactTableColumnType.NoteCodes:
                if (noteCodes) {
                    throw new SourceAssignmentException('errors.too_many_footnotes');
                }
                noteCodes = sourceInfo;
                break;
            case FactTableColumnType.Time:
            case FactTableColumnType.Dimension:
                dimensions.push(sourceInfo);
                break;
            case FactTableColumnType.Ignore:
                ignore.push(sourceInfo);
                break;
            default:
                throw new SourceAssignmentException(`errors.invalid_source_type`);
        }
    });

    return { dataValues, measure, noteCodes, dimensions, ignore };
};

async function createUpdateDimension(
    dataset: Dataset,
    factTable: FactTable,
    columnDescriptor: SourceAssignmentDTO
): Promise<void> {
    const columnInfo = await FactTableInfo.findOneByOrFail({
        columnName: columnDescriptor.column_name,
        columnIndex: columnDescriptor.column_index,
        id: factTable.id
    });
    columnInfo.columnType = columnDescriptor.column_type;
    await columnInfo.save();
    const existingDimension = dataset.dimensions.find((dim) => dim.factTableColumn === columnDescriptor.column_name);

    if (existingDimension) {
        const expectedType =
            columnInfo.columnType === FactTableColumnType.Time ? DimensionType.TimePeriod : DimensionType.Raw;
        if (existingDimension.type !== expectedType) {
            existingDimension.type = expectedType;
            await existingDimension.save();
        }
        logger.debug(
            `No Dimension to create as fact table for column ${existingDimension.factTableColumn} is already attached to one`
        );
        return;
    }

    logger.debug("The existing dimension is either a footnotes dimension or we don't have one... So lets create one");
    const dimension = new Dimension();
    dimension.type =
        columnDescriptor.column_type === FactTableColumnType.Time ? DimensionType.TimePeriod : DimensionType.Raw;
    dimension.dataset = dataset;
    dimension.factTableColumn = columnInfo.columnName;
    const savedDimension = await dimension.save();

    SUPPORTED_LOCALES.map(async (lang: string) => {
        const dimensionInfo = new DimensionInfo();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = columnInfo.columnName;
        await dimensionInfo.save();
    });
}

async function cleanupDimensions(datasetId: string, factTableInfo: FactTableInfo[]): Promise<void> {
    const dataset = await Dataset.findOneOrFail({
        where: { id: datasetId },
        relations: ['dimensions']
    });

    const revisedDimensions = dataset.dimensions;

    for (const dimension of revisedDimensions) {
        if (!factTableInfo.find((factTableInfo) => factTableInfo.columnName === dimension.factTableColumn)) {
            await dimension.remove();
        }
    }
}

async function updateFactTableInfo(factTable: FactTable, updateColumnDto: SourceAssignmentDTO) {
    const info = factTable.factTableInfo.find(
        (factTableInfo) => factTableInfo.columnName === updateColumnDto.column_name
    );
    if (!info) {
        throw new Error('No such column');
    }
    info.columnType = updateColumnDto.column_type;
    await info.save();
}

async function createUpdateMeasure(
    dataset: Dataset,
    factTable: FactTable,
    columnAssignment: SourceAssignmentDTO
): Promise<void> {
    const columnInfo = await FactTableInfo.findOneByOrFail({
        columnName: columnAssignment.column_name,
        id: factTable.id
    });
    const existingMeasure = dataset.measure;

    if (existingMeasure && existingMeasure.factTableColumn === columnAssignment.column_name) {
        logger.debug(
            `No measure to create as fact table for column ${existingMeasure.factTableColumn} is already attached to one`
        );
        return;
    }

    columnInfo.columnType = FactTableColumnType.Measure;
    await columnInfo.save();

    if (existingMeasure && existingMeasure.factTableColumn !== columnAssignment.column_name) {
        existingMeasure.factTableColumn = columnAssignment.column_name;
        await existingMeasure.save();
        return;
    }

    const measure = new Measure();
    measure.factTableColumn = columnAssignment.column_name;
    measure.dataset = dataset;
    await measure.save();
    // eslint-disable-next-line require-atomic-updates
    dataset.measure = measure;
    await dataset.save();
}

async function createUpdateNoteCodes(dataset: Dataset, factTable: FactTable, columnAssignment: SourceAssignmentDTO) {
    const columnInfo = await FactTableInfo.findOneByOrFail({
        columnName: columnAssignment.column_name,
        id: factTable.id
    });

    columnInfo.columnType = FactTableColumnType.NoteCodes;
    await columnInfo.save();

    const existingDimension = dataset.dimensions.find((dim) => dim.type === DimensionType.NoteCodes);

    if (existingDimension && existingDimension.factTableColumn === columnAssignment.column_name) {
        logger.debug(
            `No NotesCode Dimension to create as fact table for column ${existingDimension.factTableColumn} is already attached to one`
        );
        return;
    }

    if (existingDimension && existingDimension.factTableColumn !== columnAssignment.column_name) {
        existingDimension.factTableColumn = columnAssignment.column_name;
        await existingDimension.save();
        return;
    }

    const dimension = new Dimension();
    dimension.type = DimensionType.NoteCodes;
    dimension.dataset = dataset;
    dimension.factTableColumn = columnInfo.columnName;
    dimension.joinColumn = 'NoteCode';
    const savedDimension = await dimension.save();

    SUPPORTED_LOCALES.map(async (lang: string) => {
        const dimensionInfo = new DimensionInfo();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = columnInfo.columnName;
        await dimensionInfo.save();
    });
}

export const createDimensionsFromSourceAssignment = async (
    dataset: Dataset,
    factTable: FactTable,
    sourceAssignment: ValidatedSourceAssignment
): Promise<void> => {
    const { dataValues, measure, ignore, noteCodes, dimensions } = sourceAssignment;

    if (dataValues) {
        await updateFactTableInfo(factTable, dataValues);
    }

    if (noteCodes) {
        await createUpdateNoteCodes(dataset, factTable, noteCodes);
    }

    if (measure) {
        await createUpdateMeasure(dataset, factTable, measure);
    }

    await Promise.all(
        dimensions.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await createUpdateDimension(dataset, factTable, dimensionCreationDTO);
        })
    );

    await Promise.all(
        ignore.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await updateFactTableInfo(factTable, dimensionCreationDTO);
        })
    );

    await cleanupDimensions(dataset.id, factTable.factTableInfo);
};

async function createFactTableQuery(
    tableName: string,
    tempFileName: string,
    fileType: FileType,
    quack: Database
): Promise<string> {
    switch (fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFileName}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
        case FileType.Parquet:
            return `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFileName}';`;
        case FileType.Json:
        case FileType.GzipJson:
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFileName}');`;
        case FileType.Excel:
            await quack.exec('INSTALL spatial;');
            await quack.exec('LOAD spatial;');
            return `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFileName}');`;
        default:
            throw new Error('Unknown file type');
    }
}

export const validateDateTypeDimension = async (
    dimensionPatchRequest: DimensionPatchDto,
    dataset: Dataset,
    dimension: Dimension,
    factTable: FactTable
): Promise<ViewDTO | ViewErrDTO> => {
    const tableName = 'fact_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: `.${factTable.fileType}` });
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
    // Use the extracted data to try to create a reference table based on the user supplied information
    logger.debug(`Dimension patch request is: ${JSON.stringify(dimensionPatchRequest)}`);
    let dateDimensionTable: DateReferenceDataItem[] = [];
    const extractor: DateExtractor = {
        type: dimensionPatchRequest.date_type,
        yearFormat: dimensionPatchRequest.year_format,
        quarterFormat: dimensionPatchRequest.quarter_format,
        quarterTotalIsFifthQuart: dimensionPatchRequest.fifth_quarter,
        monthFormat: dimensionPatchRequest.month_format,
        dateFormat: dimensionPatchRequest.date_format
    };
    logger.debug(`Extractor created with the following JSON: ${JSON.stringify(extractor)}`);
    const previewQuery = `SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName}`;
    const preview = await quack.all(previewQuery);
    try {
        logger.debug(`Extractor created with ${JSON.stringify(extractor)}`);
        dateDimensionTable = dateDimensionReferenceTableCreator(extractor, preview);
    } catch (error) {
        logger.error(
            `Something went wrong trying to create the date reference table with the following error: ${error}`
        );
        await quack.close();
        tempFile.removeCallback();
        return {
            status: 400,
            dataset_id: dataset.id,
            errors: [
                {
                    field: 'patch',
                    tag: { name: 'errors.dimensionValidation.invalid_date_format', params: {} },
                    message: [
                        {
                            lang: Locale.English,
                            message: t('errors.dimensionValidation.invalid_date_format', { lng: Locale.English })
                        }
                    ]
                }
            ],
            extension: {
                extractor,
                totalNonMatching: preview.length,
                nonMatchingValues: []
            }
        };
    }
    // Now validate the reference table... There should no unmatched values in the fact table
    // If there are unmatched values then we need to reject the users input.
    try {
        await quack.exec(createDateDimensionTable);
        // Create the date_dimension table
        const stmt = await quack.prepare('INSERT INTO date_dimension VALUES (?,?,?,?,?);');

        dateDimensionTable.map(async (row) => {
            await stmt.run(row.dateCode, row.description, row.start, row.end, row.type);
        });
        await stmt.finalize();

        // Now validate everything matches
        const nonMatchedRows = await quack.all(
            `SELECT line_number, fact_table_date, date_dimension.date_code FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date FROM ${tableName}) as fact_table LEFT JOIN date_dimension ON fact_table.fact_table_date=date_dimension.date_code where date_code IS NULL;`
        );
        if (nonMatchedRows.length > 0) {
            if (nonMatchedRows.length === preview.length) {
                logger.error(`The user supplied an incorrect format and none of the rows matched.`);
                return {
                    status: 400,
                    dataset_id: dataset.id,
                    errors: [
                        {
                            field: 'patch',
                            tag: { name: 'errors.dimensionValidation.invalid_date_format', params: {} },
                            message: [
                                {
                                    lang: Locale.English,
                                    message: t('errors.dimensionValidation.invalid_date_format', {
                                        lng: Locale.English
                                    })
                                }
                            ]
                        }
                    ],
                    extension: {
                        extractor,
                        totalNonMatching: preview.length,
                        nonMatchingValues: []
                    }
                };
            } else {
                logger.error(
                    `There were ${nonMatchedRows.length} row(s) which didn't match based on the information given to us by the user`
                );
                const nonMatchedRowSample = await quack.all(
                    `SELECT DISTINCT fact_table_date, FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date FROM ${tableName}) as fact_table LEFT JOIN date_dimension ON fact_table.fact_table_date=date_dimension.date_code where date_code IS NULL;`
                );
                const nonMatchingValues = nonMatchedRowSample
                    .map((item) => item.fact_table_date)
                    .filter((item, i, ar) => ar.indexOf(item) === i);
                const totalNonMatching = nonMatchedRows.length;
                return {
                    status: 400,
                    errors: [
                        {
                            field: 'csv',
                            message: [
                                {
                                    lang: Locale.English,
                                    message: t('errors.dimensionValidation.unmatched_values', { lng: Locale.English })
                                },
                                {
                                    lang: Locale.Welsh,
                                    message: t('errors.dimensionValidation.unmatched_values', { lng: Locale.Welsh })
                                }
                            ],
                            tag: { name: 'errors.dimensionValidation.unmatched_values', params: {} }
                        }
                    ],
                    dataset_id: dataset.id,
                    extension: {
                        extractor,
                        totalNonMatching,
                        nonMatchingValues
                    }
                } as ViewErrDTO;
            }
        }
    } catch (error) {
        logger.error(`Something went wrong trying to validate the data with the following error: ${error}`);
        await quack.close();
        tempFile.removeCallback();
        throw error;
    }
    const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
    updateDimension.extractor = extractor;
    updateDimension.joinColumn = 'date_code';
    updateDimension.type = dimensionPatchRequest.dimension_type;
    await updateDimension.save();
    const dimensionTable = await quack.all('SELECT * FROM date_dimension;');
    await quack.close();
    tempFile.removeCallback();
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
};

async function getDatePreviewWithExtractor(
    dataset: Dataset,
    dimension: Dimension,
    factTable: FactTable,
    quack: Database,
    tableName: string
): Promise<ViewDTO> {
    const columnData = await quack.all(`SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName}`);
    const dateDimensionTable = dateDimensionReferenceTableCreator(dimension.extractor, columnData);
    await quack.exec(createDateDimensionTable);
    // Create the date_dimension table
    const stmt = await quack.prepare('INSERT INTO date_dimension VALUES (?,?,?,?,?);');
    dateDimensionTable.map(async (row) => {
        await stmt.run(row.dateCode, row.description, row.start, row.end, row.type);
    });
    await stmt.finalize();
    const dimensionTable = await quack.all(
        `SELECT * FROM date_dimension USING SAMPLE ${sampleSize} ORDER BY end_date;`
    );
    const tableHeaders = Object.keys(dimensionTable[0]);
    const dataArray = dimensionTable.map((row) => Object.values(row));
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
            total_records: dimensionTable.length,
            start_record: 1,
            end_record: dimensionTable.length < sampleSize ? dimensionTable.length : sampleSize
        },
        page_size: dimensionTable.length < sampleSize ? dimensionTable.length : sampleSize,
        total_pages: 1,
        headers,
        data: dataArray
    };
}

async function getDatePreviewWithoutExtractor(
    dataset: Dataset,
    dimension: Dimension,
    factTable: FactTable,
    quack: Database,
    tableName: string
): Promise<ViewDTO> {
    const preview = await quack.all(
        `SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`
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

export const getDateDimensionColumnPreview = async (dataset: Dataset, dimension: Dimension, factTable: FactTable) => {
    const tableName = 'fact_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: `.${factTable.fileType}` });
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
        if (dimension.extractor) {
            logger.debug('Using extractor');
            viewDto = await getDatePreviewWithExtractor(dataset, dimension, factTable, quack, tableName);
        } else {
            logger.debug('Straight column preview');
            viewDto = await getDatePreviewWithoutExtractor(dataset, dimension, factTable, quack, tableName);
        }
        await quack.close();
        tempFile.removeCallback();
        return viewDto;
    } catch (error) {
        logger.error(
            `Something went wrong trying to create the date reference table with the following error: ${error}`
        );
        await quack.close();
        tempFile.removeCallback();
        throw error;
    }
};
