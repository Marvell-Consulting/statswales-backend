import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';
import { t } from 'i18next';

import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { Dataset } from '../entities/dataset/dataset';
import { DimensionType } from '../enums/dimension-type';
import { logger } from '../utils/logger';
import { Dimension } from '../entities/dataset/dimension';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { Measure } from '../entities/dataset/measure';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DateExtractor } from '../extractors/date-extractor';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataTableDto } from '../dtos/data-table-dto';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTableColumn } from '../entities/dataset/fact-table-column';

import { DateReferenceDataItem, dateDimensionReferenceTableCreator } from './time-matching';
import { createFactTableQuery } from './cube-handler';
import { DataLakeService } from './datalake';
// eslint-disable-next-line import/no-cycle
import { getReferenceDataDimensionPreview } from './reference-data-handler';
import { duckdb } from './duckdb';

const createDateDimensionTable = `CREATE TABLE date_dimension (date_code VARCHAR, description VARCHAR, start_date datetime, end_date datetime, date_type varchar);`;
const sampleSize = 5;

export interface ValidatedSourceAssignment {
    dataValues: SourceAssignmentDTO | null;
    noteCodes: SourceAssignmentDTO | null;
    measure: SourceAssignmentDTO | null;
    dimensions: SourceAssignmentDTO[];
    ignore: SourceAssignmentDTO[];
}

export const cleanUpDimension = async (dimension: Dimension) => {
    dimension.extractor = null;
    dimension.joinColumn = null;
    dimension.type = DimensionType.Raw;
    const lookupTableId = dimension.lookupTable?.id;
    const lookupTableFilename = dimension.lookupTable?.filename;
    dimension.lookupTable = null;
    try {
        await dimension.save();
        if (lookupTableId) {
            const oldLookupTable = await LookupTable.findOneBy({ id: lookupTableId });
            await oldLookupTable?.remove();
        }
    } catch (err) {
        logger.error(
            `Something has gone wrong trying to unlink the previous lookup table from the dimension with the following error: ${err}`
        );
        throw err;
    }

    if (lookupTableId && lookupTableFilename) {
        logger.info(`Cleaning up previous lookup table`);
        try {
            const dataLakeService = new DataLakeService();
            await dataLakeService.deleteFile(lookupTableFilename, dimension.dataset.id);
        } catch (err) {
            logger.warn(`Something went wrong trying to remove previously uploaded lookup table with error: ${err}`);
        }
    }
};

export const validateSourceAssignment = (
    fileImport: DataTable,
    sourceAssignment: SourceAssignmentDTO[]
): ValidatedSourceAssignment => {
    let dataValues: SourceAssignmentDTO | null = null;
    let noteCodes: SourceAssignmentDTO | null = null;
    let measure: SourceAssignmentDTO | null = null;
    const dimensions: SourceAssignmentDTO[] = [];
    const dateDimensions: SourceAssignmentDTO[] = [];
    const ignore: SourceAssignmentDTO[] = [];

    sourceAssignment.map((sourceInfo) => {
        if (
            !fileImport.dataTableDescriptions?.find(
                (info: DataTableDescription) => info.columnName === sourceInfo.column_name
            )
        ) {
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

async function createUpdateDimension(dataset: Dataset, columnDescriptor: SourceAssignmentDTO): Promise<void> {
    const columnInfo = await FactTableColumn.findOneByOrFail({
        columnName: columnDescriptor.column_name,
        id: dataset.id
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
        const dimensionInfo = new DimensionMetadata();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = columnInfo.columnName;
        await dimensionInfo.save();
    });
}

async function cleanupDimensions(datasetId: string, factTableInfo: DataTableDescription[]): Promise<void> {
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

async function updateDataValueColumn(dataset: Dataset, dataValueColumnDto: SourceAssignmentDTO) {
    const column = await FactTableColumn.findOneByOrFail({ columnName: dataValueColumnDto.column_name, id: dataset.id });
    if (!column) {
        throw Error('No such column present in fact table');
    }
    if (column.columnType !== FactTableColumnType.DataValues) {
        column.columnType = FactTableColumnType.DataValues;
    }
    await column.save();
}

async function removeIgnoreAndUnknownColumns(dataset: Dataset, ignoreColumns: SourceAssignmentDTO[]) {
    const factTableColumns = await FactTableColumn.findBy({ dataset });
    for (const column of ignoreColumns) {
        const factTableCol = factTableColumns.find((columnInfo) => columnInfo.columnName === column.column_name);
        if (!factTableCol) {
            continue;
        }
        await factTableCol.remove();
    }
    const unknownColumns = await FactTableColumn.findBy({ dataset });
    for (const col of unknownColumns) {
        await col.remove();
    }
}

async function createUpdateMeasure(dataset: Dataset, columnAssignment: SourceAssignmentDTO): Promise<void> {
    const columnInfo = await FactTableColumn.findOneByOrFail({
        columnName: columnAssignment.column_name,
        id: dataset.id
    });

    columnInfo.columnType = FactTableColumnType.Measure;
    await columnInfo.save();
    const existingMeasure = dataset.measure;

    if (
        existingMeasure &&
        existingMeasure.factTableColumn === columnAssignment.column_name &&
        columnInfo.columnType !== columnAssignment.column_name
    ) {
        logger.debug(
            `No measure to create as fact table for column ${existingMeasure.factTableColumn} is already attached to one`
        );
        return;
    }

    if (existingMeasure && existingMeasure.factTableColumn !== columnAssignment.column_name) {
        existingMeasure.factTableColumn = columnAssignment.column_name;
        await existingMeasure.save();
        return;
    }

    const measure = new Measure();
    measure.factTableColumn = columnAssignment.column_name;
    measure.dataset = dataset;
    await measure.save();
}

async function createUpdateNoteCodes(dataset: Dataset, columnAssignment: SourceAssignmentDTO) {
    const columnInfo = await FactTableColumn.findOneByOrFail({
        columnName: columnAssignment.column_name,
        id: dataset.id
    });

    columnInfo.columnType = FactTableColumnType.NoteCodes;
    columnInfo.columnDatatype = 'VARCHAR';
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
        const dimensionInfo = new DimensionMetadata();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = columnInfo.columnName;
        await dimensionInfo.save();
    });
}

async function recreateBaseFactTable(dataset: Dataset, dataTable: DataTable): Promise<void> {
    if (dataset.factTable) {
        for (const col of dataset.factTable) {
            await FactTableColumn.getRepository().remove(col);
        }
    }
    const factTable: FactTableColumn[] = [];
    for (const col of dataTable.dataTableDescriptions) {
        const factTableCol = new FactTableColumn();
        factTableCol.columnType = FactTableColumnType.Unknown;
        factTableCol.columnName = col.columnName;
        factTableCol.columnDatatype = col.columnDatatype;
        factTableCol.columnIndex = col.columnIndex;
        factTableCol.id = dataset.id;
        factTableCol.dataset = dataset;
        const savedFactTableCol = await factTableCol.save();
        factTable.push(savedFactTableCol);
    }
}

export const createDimensionsFromSourceAssignment = async (
    dataset: Dataset,
    dataTable: DataTable,
    sourceAssignment: ValidatedSourceAssignment
): Promise<void> => {
    const { dataValues, measure, ignore, noteCodes, dimensions } = sourceAssignment;
    await recreateBaseFactTable(dataset, dataTable);
    const factTable = await FactTableColumn.findBy({ id: dataset.id });

    if (dataValues) {
        await updateDataValueColumn(dataset, dataValues);
    }

    if (noteCodes) {
        await createUpdateNoteCodes(dataset, noteCodes);
    }

    if (measure) {
        await createUpdateMeasure(dataset, measure);
    }

    await Promise.all(
        dimensions.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await createUpdateDimension(dataset, dimensionCreationDTO);
        })
    );
    await cleanupDimensions(dataset.id, dataTable.dataTableDescriptions);

    await removeIgnoreAndUnknownColumns(dataset, ignore);
};

export const validateDateTypeDimension = async (
    dimensionPatchRequest: DimensionPatchDto,
    dataset: Dataset,
    dimension: Dimension,
    factTable: DataTable
): Promise<ViewDTO | ViewErrDTO> => {
    const tableName = 'fact_table';
    const quack = await duckdb();
    const tempFile = tmp.tmpNameSync({ postfix: `.${factTable.fileType}` });
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
    logger.debug(`Extractor created with: ${JSON.stringify(extractor)}`);
    const previewQuery = `SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName}`;
    const preview = await quack.all(previewQuery);
    try {
        dateDimensionTable = dateDimensionReferenceTableCreator(extractor, preview);
        logger.debug(
            `Date dimension table created with the following JSON: ${JSON.stringify(dateDimensionTable, null, 2)}`
        );
    } catch (error) {
        logger.error(error, `Something went wrong trying to create the date reference table`);
        await quack.close();
        fs.unlinkSync(tempFile);
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
            `SELECT line_number, fact_table_date, date_dimension.date_code FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date FROM ${tableName}) as fact_table LEFT JOIN date_dimension ON CAST(fact_table.fact_table_date AS VARCHAR)=CAST(date_dimension.date_code AS VARCHAR) where date_code IS NULL;`
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
                    `SELECT DISTINCT fact_table_date, FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date FROM ${tableName}) as fact_table LEFT JOIN date_dimension ON CAST(fact_table.fact_table_date AS VARCHAR)=CAST(date_dimension.date_code AS VARCHAR) where date_code IS NULL;`
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
        fs.unlinkSync(tempFile);
        throw error;
    }
    const coverage = await quack.all(
        `SELECT MIN(start_date) as start_date, MAX(end_date) AS end_date FROM date_dimension;`
    );
    const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
    updateDataset.startDate = coverage[0].start_date;
    updateDataset.endDate = coverage[0].end_date;
    await updateDataset.save();
    const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
    updateDimension.extractor = extractor;
    updateDimension.joinColumn = 'date_code';
    updateDimension.type = dimensionPatchRequest.dimension_type;
    await updateDimension.save();
    const dimensionTable = await quack.all('SELECT * FROM date_dimension;');
    await quack.close();
    fs.unlinkSync(tempFile);
    const tableHeaders = Object.keys(dimensionTable[0]);
    const dataArray = dimensionTable.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id, { dimensions: { metadata: true } });
    const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
    const headers: CSVHeader[] = [];
    for (let i = 0; i < tableHeaders.length; i++) {
        let sourceType: FactTableColumnType;
        if (tableHeaders[i] === 'int_line_number') sourceType = FactTableColumnType.LineNumber;
        else sourceType = FactTableColumnType.Unknown;
        headers.push({
            index: i - 1,
            name: tableHeaders[i],
            source_type: sourceType
        });
    }
    return {
        dataset: DatasetDTO.fromDataset(currentDataset),
        data_table: DataTableDto.fromDataTable(currentImport),
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
    extractor: object,
    factTableColumn: string,
    dataTable: DataTable,
    quack: Database,
    tableName: string
): Promise<ViewDTO> {
    const columnData = await quack.all(`SELECT DISTINCT "${factTableColumn}" FROM ${tableName}`);
    const dateDimensionTable = dateDimensionReferenceTableCreator(extractor, columnData);
    await quack.exec(createDateDimensionTable);
    // Create the date_dimension table
    const stmt = await quack.prepare('INSERT INTO date_dimension VALUES (?,?,?,?,?);');
    dateDimensionTable.map(async (row) => {
        await stmt.run(row.dateCode, row.description, row.start, row.end, row.type);
    });
    await stmt.finalize();
    const countQuery = `SELECT COUNT(DISTINCT date_dimension.date_code) AS total_rows FROM date_dimension`;
    const countResult = await quack.all(countQuery);
    const totalRows = countResult[0].total_rows;

    const previewQuery = `
        SELECT DISTINCT(date_dimension.date_code), date_dimension.description, date_dimension.start_date, date_dimension.end_date, date_dimension.date_type
        FROM date_dimension
        RIGHT JOIN "${tableName}" ON CAST("${tableName}"."${factTableColumn}" AS VARCHAR)=CAST(date_dimension.date_code AS VARCHAR)
        ORDER BY end_date ASC
        LIMIT ${sampleSize}
    `;
    const previewResult = await quack.all(previewQuery);

    const tableHeaders = Object.keys(previewResult[0]);
    const dataArray = previewResult.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await DataTable.findOneByOrFail({ id: dataTable.id });
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
        data_table: DataTableDto.fromDataTable(currentImport),
        current_page: 1,
        page_info: {
            total_records: totalRows,
            start_record: 1,
            end_record: sampleSize
        },
        page_size: previewResult.length < sampleSize ? previewResult.length : sampleSize,
        total_pages: 1,
        headers,
        data: dataArray
    };
}

async function getPreviewWithoutExtractor(
    dataset: Dataset,
    dimension: Dimension,
    dataTable: DataTable,
    quack: Database,
    tableName: string
): Promise<ViewDTO> {
    const totals = await quack.all(
        `SELECT COUNT(DISTINCT ${dimension.factTableColumn}) AS totalLines FROM ${tableName};`
    );
    const totalLines = Number(totals[0].totalLines);

    const preview = await quack.all(
        `SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`
    );
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await DataTable.findOneByOrFail({ id: dataTable.id });
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
        data_table: DataTableDto.fromDataTable(currentImport),
        current_page: 1,
        page_info: {
            total_records: totalLines,
            start_record: 1,
            end_record: preview.length
        },
        page_size: preview.length < sampleSize ? preview.length : sampleSize,
        total_pages: 1,
        headers,
        data: dataArray
    };
}

async function getLookupPreviewWithExtractor(
    dataset: Dataset,
    dimension: Dimension,
    dataTable: DataTable,
    quack: Database
) {
    if (!dimension.lookupTable) {
        throw new Error(`Lookup table does does not exist on dimension ${dimension.id}`);
    }

    logger.debug(`Generating lookup table preview for dimension ${dimension.id}`);
    const lookupTmpFile = await getFileImportAndSaveToDisk(dataset, dimension.lookupTable);
    const lookupTableName = `lookup_table`;
    await loadFileIntoDatabase(quack, dimension.lookupTable, lookupTmpFile, lookupTableName);
    const sortColumn = (dimension.extractor as LookupTableExtractor).sortColumn || dimension.joinColumn;
    const query = `SELECT * FROM ${lookupTableName} ORDER BY ${sortColumn} LIMIT ${sampleSize};`;
    logger.debug(`Querying the cube to get the preview using query ${query}`);
    const dimensionTable = await quack.all(query);
    const tableHeaders = Object.keys(dimensionTable[0]);
    const dataArray = dimensionTable.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await DataTable.findOneByOrFail({ id: dataTable.id });
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

export const getDimensionPreview = async (
    dataset: Dataset,
    dimension: Dimension,
    dataTable: DataTable,
    lang: string
) => {
    logger.info(`Getting dimension preview for ${dimension.id}`);
    const tableName = 'fact_table';
    const quack = await duckdb();
    const tempFile = tmp.tmpNameSync({ postfix: `.${dataTable.fileType}` });
    // extract the data from the fact table
    try {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(dataTable.filename, dataset.id);
        fs.writeFileSync(tempFile, fileBuffer);
        const createTableQuery = await createFactTableQuery(tableName, tempFile, dataTable.fileType, quack);
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
        if (dimension.extractor) {
            switch (dimension.type) {
                case DimensionType.TimePoint:
                case DimensionType.TimePeriod:
                    logger.debug('Previewing a date type dimension');
                    viewDto = await getDatePreviewWithExtractor(
                        dataset,
                        dimension.extractor,
                        dimension.factTableColumn,
                        dataTable,
                        quack,
                        tableName
                    );
                    break;

                case DimensionType.LookupTable:
                    logger.debug('Previewing a lookup table');
                    viewDto = await getLookupPreviewWithExtractor(dataset, dimension, dataTable, quack);
                    break;

                case DimensionType.ReferenceData:
                    logger.debug('Previewing a lookup table');
                    viewDto = await getReferenceDataDimensionPreview(
                        dataset,
                        dimension,
                        dataTable,
                        quack,
                        tableName,
                        lang
                    );
                    break;

                default:
                    logger.debug(`Previewing a dimension of an unknown type.  Type supplied is ${dimension.type}`);
                    viewDto = await getPreviewWithoutExtractor(dataset, dimension, dataTable, quack, tableName);
            }
        } else {
            logger.debug('Straight column preview');
            viewDto = await getPreviewWithoutExtractor(dataset, dimension, dataTable, quack, tableName);
        }
        fs.unlinkSync(tempFile);
        return viewDto;
    } catch (error) {
        logger.error(`Something went wrong trying to create dimension preview with the following error: ${error}`);
        fs.unlinkSync(tempFile);
        throw error;
    } finally {
        await quack.close();
    }
};
