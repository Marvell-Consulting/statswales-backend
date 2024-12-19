import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';

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
        await measure.save();
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
    if (tableMatcher) {
        return {
            sortColumn: tableMatcher.sort_column,
            formatColumn: tableMatcher.format_column,
            measureTypeColumn: tableMatcher.measure_type_column,
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
        const formatColumn: string | undefined = protoLookupTable.factTableInfo.find((info) =>
            info.columnName.toLowerCase().startsWith('format')
        )?.columnName;
        const measureTypeColumn: string | undefined = protoLookupTable.factTableInfo.find((info) =>
            info.columnName.toLowerCase().startsWith('measure')
        )?.columnName;
        if (!formatColumn) throw new Error('Could not find a format column in the lookup table');
        if (!measureTypeColumn) throw new Error('Could not find a measure type column in the lookup table');
        return {
            sortColumn: protoLookupTable.factTableInfo.find((info) => info.columnName.toLowerCase().startsWith('sort'))
                ?.columnName,
            formatColumn,
            measureTypeColumn,
            descriptionColumns: protoLookupTable.factTableInfo
                .filter((info) => info.columnName.toLowerCase().startsWith('description'))
                .map((info) => columnIdentification(info)),
            notesColumns: protoLookupTable.factTableInfo
                .filter((info) => info.columnName.toLowerCase().startsWith('note'))
                .map((info) => columnIdentification(info))
        };
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

    let confirmedJoinColumn: string;
    if (tableMatcher?.join_column) {
        confirmedJoinColumn = tableMatcher.join_column;
    } else {
        const possibleJoinColumns = protoLookupTable.factTableInfo.filter((info) => {
            if (info.columnName.toLowerCase().startsWith('measure')) return false;
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
        confirmedJoinColumn = possibleJoinColumns[0].columnName;
    }

    try {
        const nonMatchedRows = await quack.all(
            `SELECT line_number, fact_table_column, ${lookupTableName}.${confirmedJoinColumn} as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${measure.factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
            WHERE lookup_table_column IS NULL;`
        );
        const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
        if (nonMatchedRows.length === rows[0].total_rows) {
            logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT ${measure.factTableColumn} FROM ${factTableName};`
            );
            return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
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
            return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
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
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
            totalNonMatching: nonMatchedRows[0].total_rows,
            nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
        });
    }

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
