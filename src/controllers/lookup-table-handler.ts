import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';
import { t } from 'i18next';

import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTable } from '../entities/dataset/fact-table';
import { Dimension } from '../entities/dataset/dimension';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { Locale } from '../enums/locale';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DimensionType } from '../enums/dimension-type';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';
import { DataLakeService } from '../services/datalake';

// eslint-disable-next-line import/no-cycle
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from './cube-handler';

interface ColumnDescriptor {
    lang: string;
    name: string;
}
export interface LookupTableExtractor {
    sortColumn: string | undefined;
    descriptionColumns: ColumnDescriptor[];
    notesColumns: ColumnDescriptor[];
}

function convertFactTableToLookupTable(factTable: FactTable, dimension: Dimension) {
    const lookupTable = new LookupTable();
    lookupTable.id = factTable.id;
    lookupTable.fileType = factTable.fileType;
    lookupTable.filename = factTable.filename;
    lookupTable.mimeType = factTable.mimeType;
    lookupTable.hash = factTable.hash;
    lookupTable.delimiter = factTable.delimiter;
    lookupTable.linebreak = factTable.linebreak;
    lookupTable.quote = factTable.quote;
    lookupTable.dimension = dimension;
    return lookupTable;
}

export const validateLookupTable = async (
    protoLookupTable: FactTable,
    factTable: FactTable,
    dataset: Dataset,
    dimension: Dimension,
    buffer: Buffer,
    joinColumn?: string
): Promise<ViewDTO | ViewErrDTO> => {
    const lookupTable = convertFactTableToLookupTable(protoLookupTable, dimension);
    const factTableName = 'fact_table';
    const lookupTableName = 'preview_lookup';
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
    if (joinColumn) {
        confirmedJoinColumn = joinColumn;
    } else {
        const possibleJoinColumns = protoLookupTable.factTableInfo.filter((info) => {
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
            FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
            WHERE lookup_table_column IS NULL;`
        );
        const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
        if (nonMatchedRows.length === rows[0].total_rows) {
            logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT ${dimension.factTableColumn} FROM ${factTableName};`
            );
            return {
                status: 400,
                dataset_id: dataset.id,
                errors: [
                    {
                        field: 'patch',
                        tag: { name: 'errors.dimensionValidation.invalid_lookup_table', params: {} },
                        message: [
                            {
                                lang: Locale.English,
                                message: t('errors.dimensionValidation.invalid_lookup_table', {
                                    lng: Locale.English
                                })
                            }
                        ]
                    }
                ],
                extension: {
                    totalNonMatching: rows[0].total_rows,
                    nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
                }
            };
        }
        if (nonMatchedRows.length > 0) {
            const nonMatchedValues = await quack.all(
                `SELECT DISTINCT fact_table_column FROM (SELECT "${dimension.factTableColumn}" as fact_table_column FROM ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR) where lookup_table_column IS NULL;`
            );
            logger.error(
                `The user supplied an incorrect or incomplete lookup table and ${nonMatchedRows.length} rows didn't match`
            );
            return {
                status: 400,
                dataset_id: dataset.id,
                errors: [
                    {
                        field: 'patch',
                        tag: { name: 'errors.dimensionValidation.invalid_lookup_table', params: {} },
                        message: [
                            {
                                lang: Locale.English,
                                message: t('errors.dimensionValidation.invalid_lookup_table', {
                                    lng: Locale.English
                                })
                            }
                        ]
                    }
                ],
                extension: {
                    totalNonMatching: nonMatchedRows.length,
                    nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
                }
            };
        }
    } catch (error) {
        logger.error(
            `Something went wrong, most likely an incorrect join column name, while trying to validate the lookup table with error: ${error}`
        );
        const nonMatchedRows = await quack.all(`SELECT COUNT(*) AS total_rows FROM ${factTableName};`);
        const nonMatchedValues = await quack.all(`SELECT DISTINCT ${dimension.factTableColumn} FROM ${factTableName};`);
        return {
            status: 400,
            dataset_id: dataset.id,
            errors: [
                {
                    field: 'patch',
                    tag: { name: 'errors.dimensionValidation.invalid_join_column', params: {} },
                    message: [
                        {
                            lang: Locale.English,
                            message: t('errors.dimensionValidation.invalid_join_column', {
                                lng: Locale.English
                            })
                        }
                    ]
                }
            ],
            extension: {
                totalNonMatching: nonMatchedRows[0].total_rows,
                nonMatchingValues: nonMatchedValues.map((row) => Object.values(row)[0])
            }
        };
    }
    // Clean up previously uploaded dimensions
    if (dimension.lookupTable) {
        logger.info(`Cleaning up previous lookup table`);
        try {
            const dataLakeService = new DataLakeService();
            await dataLakeService.deleteFile(dimension.lookupTable.filename, dataset.id);
        } catch (err) {
            logger.warn(`Something went wrong trying to remove previously uploaded lookup table with error: ${err}`);
        }

        try {
            const lookupTableId = dimension.lookupTable.id;
            dimension.lookupTable = null;
            dimension.extractor = null;
            dimension.type = DimensionType.Raw;
            dimension.joinColumn = null;
            await dimension.save();
            const oldLookupTable = await LookupTable.findOneBy({ id: lookupTableId });
            await oldLookupTable?.remove();
        } catch (err) {
            logger.error(
                `Something has gone wrong trying to unlink the previous lookup table from the dimension with the following error: ${err}`
            );
            throw err;
        }
    }

    lookupTable.isStatsWales2Format = !protoLookupTable.factTableInfo.find((info) =>
        info.columnName.toLowerCase().startsWith('lang')
    );
    const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
    updateDimension.type = DimensionType.LookupTable;
    updateDimension.joinColumn = confirmedJoinColumn;
    updateDimension.lookupTable = lookupTable;
    const extractor: LookupTableExtractor = {
        sortColumn: protoLookupTable.factTableInfo.find((info) => info.columnName.toLowerCase().startsWith('sort'))
            ?.columnName,
        descriptionColumns: protoLookupTable.factTableInfo
            .filter((info) => info.columnName.toLowerCase().startsWith('description'))
            .map((info) => {
                return {
                    name: info.columnName,
                    lang: info.columnName.split('_')[1]
                };
            }),
        notesColumns: protoLookupTable.factTableInfo
            .filter((info) => info.columnName.toLowerCase().startsWith('note'))
            .map((info) => {
                return {
                    name: info.columnName,
                    lang: info.columnName.split('_')[1]
                };
            })
    };

    updateDimension.extractor = extractor;
    logger.debug('Saving the lookup table');
    await lookupTable.save();
    logger.debug('Saving the dimension');
    updateDimension.lookupTable = lookupTable;
    updateDimension.type = DimensionType.LookupTable;
    await updateDimension.save();

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
