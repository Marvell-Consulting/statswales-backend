import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';

import { DimensionType } from '../enums/dimension-type';
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTable } from '../entities/dataset/fact-table';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { columnIdentification, convertFactTableToLookupTable, lookForJoinColumn } from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { logger } from '../utils/logger';
import { Dimension } from '../entities/dataset/dimension';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { viewErrorGenerator } from '../utils/view-error-generator';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';

import { cleanUpDimension } from './dimension-processor';

async function setupDimension(
    dimension: Dimension,
    lookupTable: LookupTable,
    protoLookupTable: FactTable,
    confirmedJoinColumn: string,
    tableMatcher?: LookupTablePatchDTO
) {
    // Clean up previously uploaded dimensions
    if (dimension.lookupTable) await cleanUpDimension(dimension);
    lookupTable.isStatsWales2Format = !protoLookupTable.factTableInfo.find((info) =>
        info.columnName.toLowerCase().startsWith('lang')
    );
    const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
    updateDimension.type = DimensionType.LookupTable;
    updateDimension.joinColumn = confirmedJoinColumn;
    updateDimension.lookupTable = lookupTable;
    logger.debug(`Creating extractor...`);
    updateDimension.extractor = createExtractor(protoLookupTable, tableMatcher);
    logger.debug('Saving the lookup table');
    await lookupTable.save();
    logger.debug('Saving the dimension');
    updateDimension.lookupTable = lookupTable;
    updateDimension.type = DimensionType.LookupTable;
    await updateDimension.save();
}

function createExtractor(protoLookupTable: FactTable, tableMatcher?: LookupTablePatchDTO): LookupTableExtractor {
    if (tableMatcher?.description_columns) {
        logger.debug(`Table matcher is supplied using user supplied information to create extractor...`);
        return {
            sortColumn: tableMatcher.sort_column,
            hierarchyColumn: tableMatcher.hierarchy,
            descriptionColumns: tableMatcher.description_columns.map(
                (desc) =>
                    protoLookupTable.factTableInfo
                        .filter((info) => info.columnName === desc)
                        .map((info) => columnIdentification(info))[0]
            ),
            notesColumns: tableMatcher.notes_column?.map(
                (desc) =>
                    protoLookupTable.factTableInfo
                        .filter((info) => info.columnName === desc)
                        .map((info) => columnIdentification(info))[0]
            )
        };
    } else {
        logger.debug(`Using lookup table to try try to generate the extractor...`);
        const sortColumn = protoLookupTable.factTableInfo.find((info) =>
            info.columnName.toLowerCase().startsWith('sort')
        )?.columnName;
        const hierarchyColumn = protoLookupTable.factTableInfo.find((info) =>
            info.columnName.toLowerCase().startsWith('hierarchy')
        )?.columnName;
        const filteredDescriptionColumns = protoLookupTable.factTableInfo.filter((info) =>
            info.columnName.toLowerCase().startsWith('description')
        );
        if (filteredDescriptionColumns.length < 1) {
            throw new Error('Could not identify description columns in lookup table');
        }
        const descriptionColumns = filteredDescriptionColumns.map((info) => columnIdentification(info));
        const filteredNotesColumns = protoLookupTable.factTableInfo.filter((info) =>
            info.columnName.toLowerCase().startsWith('note')
        );
        let notesColumns: ColumnDescriptor[] | undefined;
        if (filteredNotesColumns.length > 0) {
            notesColumns = filteredNotesColumns.map((info) => columnIdentification(info));
        }
        return {
            sortColumn,
            hierarchyColumn,
            descriptionColumns,
            notesColumns
        };
    }
}

export const validateLookupTable = async (
    protoLookupTable: FactTable,
    factTable: FactTable,
    dataset: Dataset,
    dimension: Dimension,
    buffer: Buffer,
    tableMatcher?: LookupTablePatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
    const lookupTable = convertFactTableToLookupTable(protoLookupTable, dimension);
    const factTableName = 'fact_table';
    const lookupTableName = 'preview_lookup';
    const quack = await Database.create(':memory:');
    const lookupTableTmpFile = tmp.fileSync({ postfix: `.${lookupTable.fileType}` });
    try {
        logger.debug(`Writing the lookup table to disk: ${lookupTableTmpFile.name}`);
        fs.writeFileSync(lookupTableTmpFile.name, buffer);
        const factTableTmpFile = await getFileImportAndSaveToDisk(dataset, factTable);
        logger.debug(`Loading fact table in to DuckDB`);
        await loadFileIntoDatabase(quack, factTable, factTableTmpFile, factTableName);
        logger.debug(`Loading lookup table in to DuckDB`);
        await loadFileIntoDatabase(quack, lookupTable, lookupTableTmpFile, lookupTableName);
        lookupTableTmpFile.removeCallback();
        factTableTmpFile.removeCallback();
    } catch (err) {
        logger.error(`Something went wrong trying to load data in to DuckDB with the following error: ${err}`);
        throw err;
    }

    let confirmedJoinColumn: string | undefined;
    try {
        confirmedJoinColumn = lookForJoinColumn(protoLookupTable, dimension.factTableColumn, tableMatcher);
    } catch (err) {
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.no_join_column', {});
    }

    if (!confirmedJoinColumn) {
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.no_join_column', {});
    }

    try {
        logger.debug(`Validating the lookup table`);
        const nonMatchedRows = await quack.all(
            `SELECT line_number, fact_table_column, ${lookupTableName}.${confirmedJoinColumn} as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
            WHERE lookup_table_column IS NULL;`
        );
        logger.debug(`Number of rows from non matched rows query: ${nonMatchedRows.length}`);
        const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
        if (nonMatchedRows.length === rows[0].total_rows) {
            logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT ${dimension.factTableColumn} FROM ${factTableName};`
            );
            return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
                totalNonMatching: rows[0].total_rows,
                nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
            });
        }
        if (nonMatchedRows.length > 0) {
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT fact_table_column FROM (SELECT "${dimension.factTableColumn}" as fact_table_column
                FROM ${factTableName})as fact_table
                LEFT JOIN ${lookupTableName}
                ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
                WHERE ${lookupTableName}."${confirmedJoinColumn}" IS NULL;`
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
        const nonMatchedValues = await quack.all(`SELECT DISTINCT ${dimension.factTableColumn} FROM ${factTableName};`);
        return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
            totalNonMatching: nonMatchedRows[0].total_rows,
            nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
        });
    }

    logger.debug(`Lookup table passed validation.  Setting up dimension.`);
    await setupDimension(dimension, lookupTable, protoLookupTable, confirmedJoinColumn, tableMatcher);

    try {
        logger.debug('Passed validation preparing to send back the preview');
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