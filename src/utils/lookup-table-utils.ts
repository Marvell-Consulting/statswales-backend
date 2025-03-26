import { DataTable } from '../entities/dataset/data-table';
import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { SupportedLanguagues } from '../enums/locale';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';

import { logger } from './logger';
import { Database } from 'duckdb-async';
import { Dataset } from '../entities/dataset/dataset';
import { ViewErrDTO } from '../dtos/view-dto';
import { viewErrorGenerators } from './view-error-generators';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { DataValueFormat } from '../enums/data-value-format';

export function convertDataTableToLookupTable(dataTable: DataTable) {
  const lookupTable = new LookupTable();
  lookupTable.id = dataTable.id;
  lookupTable.fileType = dataTable.fileType;
  lookupTable.filename = dataTable.filename;
  lookupTable.mimeType = dataTable.mimeType;
  lookupTable.hash = dataTable.hash;
  return lookupTable;
}

export function columnIdentification(info: DataTableDescription) {
  let lang = 'zz';
  for (const supLang of Object.values(SupportedLanguagues)) {
    if (info.columnName.toLowerCase().endsWith(supLang.code.toLowerCase())) {
      lang = supLang.code;
      break;
    } else if (info.columnName.toLowerCase().endsWith(supLang.name.toLowerCase())) {
      lang = supLang.code;
      break;
    }
  }
  return {
    name: info.columnName,
    lang
  };
}

// Look for the join column.  If there's a table matcher we always use this
// If the user has called the lookup table column the same as the fact table column use this
// If they've used the exact name in the guidance e.g. ref_code, reference_code, refcode use this
// Finally we do fuzzy matching where we exclude everything that isn't a protected name and see what we have left
export const lookForJoinColumn = (
  protoLookupTable: DataTable,
  factTableColumn: string,
  tableMatcher?: MeasureLookupPatchDTO | LookupTablePatchDTO
): string => {
  const refCol = protoLookupTable.dataTableDescriptions.find((col) => col.columnName.toLowerCase().startsWith('ref'));
  if (tableMatcher?.join_column) {
    return tableMatcher.join_column;
  } else if (refCol) {
    return refCol.columnName;
  } else if (
    protoLookupTable.dataTableDescriptions.find((col) => col.columnName.toLowerCase() === factTableColumn.toLowerCase())
  ) {
    return factTableColumn;
  } else {
    const possibleJoinColumns = protoLookupTable.dataTableDescriptions.filter((info) => {
      if (info.columnName.toLowerCase().includes('decimal')) return false;
      if (info.columnName.toLowerCase().includes('hierarchy')) return false;
      if (info.columnName.toLowerCase().includes('format')) return false;
      if (info.columnName.toLowerCase().includes('description')) return false;
      if (info.columnName.toLowerCase().includes('sort')) return false;
      if (info.columnName.toLowerCase().includes('note')) return false;
      if (info.columnName.toLowerCase().includes('type')) return false;
      if (info.columnName.toLowerCase().includes('lang')) return false;
      logger.debug(`Looks like column ${info.columnName.toLowerCase()} is a join column`);
      return true;
    });
    if (possibleJoinColumns.length > 1) {
      throw new Error(
        `There are to many possible join columns.  Ask user for more information... Join columns present: ${possibleJoinColumns.join(', ')}`
      );
    }
    if (possibleJoinColumns.length === 0) {
      throw new Error('Could not find a column to join against the fact table.');
    }
    logger.debug(`Found the following join column ${JSON.stringify(possibleJoinColumns)}`);
    return possibleJoinColumns[0].columnName;
  }
};

export const validateLookupTableLanguages = async (
  quack: Database,
  dataset: Dataset,
  joinColumn: string,
  lookupTableName: string,
  validationType: string
): Promise<ViewErrDTO | undefined> => {
  try {
    logger.debug(`Adding primary key of ${joinColumn} and language to lookup table`);
    await quack.exec(`ALTER TABLE "${lookupTableName}" ADD PRIMARY KEY ("${joinColumn}", language);`);
  } catch (error) {
    logger.error(error, `Something went wrong trying to add primary key to lookup table`);
    return viewErrorGenerators(400, dataset.id, 'patch', `errors.${validationType}_validation.primary_key_failed`, {});
  }

  try {
    logger.debug(`Checking language counts match total number of supported languages`);
    const missingLanguageRows = await quack.all(`
      SELECT "${joinColumn}", COUNT(language) as lang_count
      FROM "${lookupTableName}"
      GROUP BY "${joinColumn}" HAVING lang_count < ${SUPPORTED_LOCALES.length};
    `);
    if (missingLanguageRows.length > 0) {
      const missingLanguages: string[] = [];
      SUPPORTED_LOCALES.forEach((locale) => {
        if (!missingLanguageRows.find((row) => row.languages.includes(locale.split('-')[0]))) {
          missingLanguages.push(locale);
        }
      });
      logger.error(`The lookup table is missing the following languages: ${missingLanguages.join(', ')}`);
      return viewErrorGenerators(
        400,
        dataset.id,
        'patch',
        `errors.${validationType}_validation.missing_languages`,
        missingLanguages
      );
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to check language counts`);
    return viewErrorGenerators(500, dataset.id, 'patch', `errors.${validationType}_validation.unknown_error`, {});
  }

  try {
    logger.debug(`Checking descriptions and notes are different between languages`);
    const duplicateDescriptionRows = await quack.all(`
      SELECT description, COUNT(language) as lang_count
      FROM (SELECT * FROM "${lookupTableName}" where description IS NOT NULL)
      GROUP BY description HAVING lang_count > 1
    `);
    const duplicateNoteRows = await quack.all(`
      SELECT notes, COUNT(language) as lang_count
      FROM (SELECT * FROM "${lookupTableName}" WHERE notes IS NOT NULL)
      GROUP BY notes HAVING lang_count > 1
    `);
    if (duplicateDescriptionRows.length > 0 || duplicateNoteRows.length > 0) {
      logger.error(`The lookup table has duplicate descriptions or notes`);
      logger.error(`Duplicate descriptions: ${JSON.stringify(duplicateDescriptionRows)}`);
      logger.error(`Duplicate notes: ${JSON.stringify(duplicateNoteRows)}`);
      return viewErrorGenerators(
        400,
        dataset.id,
        'patch',
        `errors.${validationType}_validation.duplicate_descriptions_or_notes`,
        {}
      );
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to check descriptions and notes`);
    return viewErrorGenerators(500, dataset.id, 'patch', `errors.${validationType}_validation.unknown_error`, {});
  }
  return undefined;
};

export const validateLookupTableReferenceValues = async (
  quack: Database,
  dataset: Dataset,
  factTableColumn: string,
  joinColumn: string,
  lookupTableName: string,
  factTableName: string,
  validationType: string
): Promise<ViewErrDTO | undefined> => {
  try {
    logger.debug(`Validating the lookup table`);
    const nonMatchedRows = await quack.all(
      `SELECT line_number, fact_table_column, "${lookupTableName}".${joinColumn} as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN "${lookupTableName}" ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST("${lookupTableName}"."${joinColumn}" AS VARCHAR)
            WHERE lookup_table_column IS NULL;`
    );
    logger.debug(`Number of rows from non matched rows query: ${nonMatchedRows.length}`);
    const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
    if (nonMatchedRows.length === rows[0].total_rows) {
      logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
      const nonMatchedFactTableValues = await quack.all(`SELECT DISTINCT ${factTableColumn} FROM ${factTableName};`);
      const nonMatchedLookupValues = await quack.all(`SELECT DISTINCT "${joinColumn}" FROM "${lookupTableName}";`);
      return viewErrorGenerators(400, dataset.id, 'patch', `errors.${validationType}_validation.no_reference_match`, {
        totalNonMatching: rows[0].total_rows,
        nonMatchingDataTableValues: nonMatchedFactTableValues.map((row) => Object.values(row)[0]),
        nonMatchedLookupValues: nonMatchedLookupValues.map((row) => Object.values(row)[0]),
        mismatch: true
      });
    }
    if (nonMatchedRows.length > 0) {
      const nonMatchingDataTableValues = await quack.all(
        `SELECT DISTINCT fact_table_column FROM (SELECT "${factTableColumn}" as fact_table_column
                FROM ${factTableName}) as fact_table
                LEFT JOIN "${lookupTableName}"
                ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST("${lookupTableName}"."${joinColumn}" AS VARCHAR)
                WHERE "${lookupTableName}"."${joinColumn}" IS NULL;`
      );
      const nonMatchingLookupValues = await quack.all(
        `SELECT DISTINCT lookup_table_column FROM (SELECT "${joinColumn}" as lookup_table_column
                 FROM "${lookupTableName}") AS lookup_table
                 LEFT JOIN ${factTableName} ON CAST(lookup_table.lookup_table_column AS VARCHAR)=CAST(${factTableName}."${factTableColumn}" AS VARCHAR)
                 WHERE ${factTableName}."${factTableColumn}" IS NULL;`
      );
      logger.error(
        `The user supplied an incorrect or incomplete lookup table and ${nonMatchedRows.length} rows didn't match`
      );
      return viewErrorGenerators(
        400,
        dataset.id,
        'patch',
        `errors.${validationType}_validation.some_references_failed_to_match`,
        {
          totalNonMatching: nonMatchedRows.length,
          nonMatchingDataTableValues: nonMatchingDataTableValues.map((row) => Object.values(row)[0]),
          nonMatchedLookupValues: nonMatchingLookupValues.map((row) => Object.values(row)[0]),
          mismatch: true
        }
      );
    }
  } catch (error) {
    logger.error(
      error,
      `Something went wrong, most likely an incorrect join column name, while trying to validate the lookup table.`
    );
    const nonMatchedRows = await quack.all(`SELECT COUNT(*) AS total_rows FROM ${factTableName};`);
    const nonMatchedValues = await quack.all(`SELECT DISTINCT ${factTableColumn} FROM ${factTableName};`);
    return viewErrorGenerators(500, dataset.id, 'patch', `errors.${validationType}_validation.unknown_error`, {
      totalNonMatching: nonMatchedRows[0].total_rows,
      nonMatchingDataTableValues: nonMatchedValues.map((row) => Object.values(row)[0]),
      mismatch: true
    });
  }
  return undefined;
};

async function checkDecimalColumn(quack: Database, extractor: MeasureLookupTableExtractor, lookupTableName: string) {
  const unmatchedFormats: string[] = [];
  logger.debug('Decimal column is present.  Validating contains only integers.');
  const formats = await quack.all(`SELECT DISTINCT "${extractor.decimalColumn}" as formats FROM ${lookupTableName};`);
  for (const format of Object.values(formats.map((format) => format.formats))) {
    if (!Number.isInteger(Number(format)) && Number(format) >= 0) unmatchedFormats.push(format);
  }
  return unmatchedFormats;
}

async function checkFormatColumn(quack: Database, extractor: MeasureLookupTableExtractor, lookupTableName: string) {
  const unmatchedFormats: string[] = [];
  logger.debug('Decimal column is present.  Validating contains only integers.');
  const formats = await quack.all(`SELECT DISTINCT "${extractor.formatColumn}" as formats FROM ${lookupTableName};`);
  logger.debug(`Formats = ${JSON.stringify(Object.values(DataValueFormat), null, 2)}`);
  for (const format of Object.values(formats.map((format) => format.formats))) {
    if (Object.values(DataValueFormat).indexOf(format.toLowerCase()) === -1) unmatchedFormats.push(format);
  }
  return unmatchedFormats;
}

export const validateMeasureTableContent = async (
  quack: Database,
  datasetId: string,
  lookupTableName: string,
  extractor: MeasureLookupTableExtractor
): Promise<ViewErrDTO | undefined> => {
  if (extractor.formatColumn && extractor.formatColumn.toLowerCase().indexOf('format') > -1) {
    logger.debug('Formats column is present.  Validating all formats present are valid.');
    const unMatchedFormats = await checkFormatColumn(quack, extractor, lookupTableName);
    if (unMatchedFormats.length > 0) {
      logger.debug(
        `Found invalid formats while validating format column.  Formats found: ${JSON.stringify(unMatchedFormats)}`
      );
      return viewErrorGenerators(400, datasetId, 'patch', 'errors.measure_validation.invalid_formats_present', {
        totalNonMatching: unMatchedFormats.length,
        nonMatchingValues: unMatchedFormats,
        mismatch: true
      });
    }
  }

  if (extractor.decimalColumn && extractor.decimalColumn.toLowerCase().indexOf('decimal') !== -1) {
    const unmatchedDecimals = await checkDecimalColumn(quack, extractor, lookupTableName);
    if (unmatchedDecimals.length > 0) {
      logger.debug(
        `Found invalid formats while validating decimals column.  Formats found: ${JSON.stringify(unmatchedDecimals)}`
      );
      return viewErrorGenerators(400, datasetId, 'patch', 'errors.measure_validation.invalid_decimals_present', {
        totalNonMatching: unmatchedDecimals.length,
        nonMatchingValues: unmatchedDecimals,
        mismatch: true
      });
    }
  }
  logger.debug('Validating column contents complete.');
  return undefined;
};
