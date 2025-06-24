import { format as pgformat } from '@scaleleap/pg-format';

import { DataTable } from '../entities/dataset/data-table';
import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { Locale } from '../enums/locale';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';

import { logger } from './logger';
import { Database, TableData } from 'duckdb-async';
import { Dataset } from '../entities/dataset/dataset';
import { ViewErrDTO } from '../dtos/view-dto';
import { viewErrorGenerators } from './view-error-generators';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { DataValueFormat } from '../enums/data-value-format';
import { t } from 'i18next';

export function convertDataTableToLookupTable(dataTable: DataTable): LookupTable {
  const lookupTable = new LookupTable();
  lookupTable.id = dataTable.id;
  lookupTable.fileType = dataTable.fileType;
  lookupTable.filename = dataTable.filename;
  lookupTable.mimeType = dataTable.mimeType;
  lookupTable.encoding = dataTable.encoding;
  lookupTable.originalFilename = dataTable.originalFilename;
  lookupTable.hash = dataTable.hash;
  return lookupTable;
}

interface ColumnIdentification {
  name: string;
  lang: string;
}

export function columnIdentification(info: DataTableDescription): ColumnIdentification {
  let columnLang = 'zz';
  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.split('-')[0].toLowerCase();
    const columnName = info.columnName.toLowerCase();
    if (columnName.endsWith(locale.split('-')[0].toLowerCase())) {
      columnLang = locale.toLowerCase();
      break;
    }
    for (const nestedLocale of SUPPORTED_LOCALES) {
      if (columnName.endsWith(t(`language.${lang}`, { lng: nestedLocale }).toLowerCase())) {
        columnLang = locale.toLowerCase();
        break;
      }
    }
    if (columnLang === lang) break;
  }
  return {
    name: info.columnName,
    lang: columnLang
  };
}

export const languageMatcherCaseStatement = (languageColumn: string | undefined): string => {
  if (!languageColumn) return `''`;
  const languageMatcher: string[] = [];
  SUPPORTED_LOCALES.map((locale) => {
    const lang = locale.split('-')[0].toLowerCase();
    const tLang = lang;
    languageMatcher.push(pgformat('WHEN LOWER(%I) LIKE %L THEN %L', languageColumn, `%${lang}%`, locale.toLowerCase()));
    SUPPORTED_LOCALES.map((locale) => {
      const lang = locale.split('-')[0].toLowerCase();
      languageMatcher.push(
        pgformat(
          'WHEN LOWER(%I) LIKE %L THEN %L',
          languageColumn,
          `%${t(`language.${lang}`, { lng: tLang }).toLowerCase()}%`,
          locale.toLowerCase()
        )
      );
    });
  });
  return `CASE
  ${languageMatcher.join('\n')}
  END`;
};

// Look for the join column.  If there's a table matcher we always use this
// If the user has called the lookup table column the same as the fact table column use this
// If they've used the exact name in the guidance e.g. ref_code, reference_code, refcode use this
// Finally we do fuzzy matching where we exclude everything that isn't a protected name and see what we have left
export const lookForJoinColumn = (
  protoLookupTable: DataTable,
  factTableColumn: string,
  tableLanguage: Locale,
  tableMatcher?: MeasureLookupPatchDTO | LookupTablePatchDTO
): string => {
  const refCol = protoLookupTable.dataTableDescriptions.find((col) => col.columnName.toLowerCase().startsWith('ref'));
  const refCodeCol = protoLookupTable.dataTableDescriptions.find((col) =>
    col.columnName.toLowerCase().includes(t('lookup_column_headers.refcode', { lng: tableLanguage }).toLowerCase())
  );
  if (tableMatcher?.join_column) {
    return tableMatcher.join_column;
  } else if (refCol) {
    return refCol.columnName;
  } else if (refCodeCol) {
    return refCodeCol.columnName;
  } else if (
    protoLookupTable.dataTableDescriptions.find((col) => col.columnName.toLowerCase() === factTableColumn.toLowerCase())
  ) {
    return factTableColumn;
  } else {
    const possibleJoinColumns = protoLookupTable.dataTableDescriptions.filter((info) => {
      const columnName = info.columnName.toLowerCase();
      if (columnName.includes(t('lookup_column_headers.decimal', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.hierarchy', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.format', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.description', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.sort', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.notes', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.type', { lng: tableLanguage }))) return false;
      if (columnName.includes(t('lookup_column_headers.lang', { lng: tableLanguage }))) return false;
      if (columnName.includes('lang')) return false;

      logger.debug(`Looks like column ${columnName} is a join column`);
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
    // logger.debug(`Found the following join column ${JSON.stringify(possibleJoinColumns)}`);
    return possibleJoinColumns[0].columnName;
  }
};

export const validateLookupTableLanguages = async (
  quack: Database,
  dataset: Dataset,
  revisionId: string,
  joinColumn: string,
  lookupTableName: string,
  validationType: string
): Promise<ViewErrDTO | undefined> => {
  try {
    logger.debug(`Adding primary key of ${joinColumn} and language to lookup table`);
    const alterTableQuery = pgformat(
      'ALTER TABLE %I.%I ADD PRIMARY KEY (%I, language);',
      revisionId,
      lookupTableName,
      joinColumn
    );
    const execQuery = pgformat(`CALL postgres_execute(%I, %L);`, 'postgres_db', alterTableQuery);
    // logger.debug(`Executing query: ${execQuery}`);
    await quack.exec(execQuery);
  } catch (error) {
    logger.error(error, `Something went wrong trying to add primary key to lookup table`);
    return viewErrorGenerators(400, dataset.id, 'patch', `errors.${validationType}_validation.primary_key_failed`, {});
  }

  try {
    logger.debug(`Checking language counts match total number of supported languages`);
    const missingLanguageRows = await quack.all(`
      SELECT "${joinColumn}", COUNT(language) as lang_count, STRING_AGG(language, ',') as languages
      FROM "${lookupTableName}"
      GROUP BY "${joinColumn}" HAVING lang_count < ${SUPPORTED_LOCALES.length};
    `);
    if (missingLanguageRows.length > 0) {
      const missingLanguages: string[] = [];
      SUPPORTED_LOCALES.forEach((locale) => {
        // logger.debug(`Checking if ${locale.toLowerCase()} is missing from ${JSON.stringify(missingLanguageRows)}`);
        if (!missingLanguageRows.find((row) => row.languages.includes(locale.split('-')[0].toLowerCase()))) {
          missingLanguages.push(`languages.${locale.split('-')[0]}`);
        }
      });
      logger.error(`The lookup table is missing the following languages: ${missingLanguages.join(', ')}`);
      return viewErrorGenerators(
        400,
        dataset.id,
        'patch',
        `errors.${validationType}_validation.missing_languages`,
        {},
        { languages: missingLanguages.join(', ') }
      );
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to check language counts`);
    return viewErrorGenerators(500, dataset.id, 'patch', `errors.${validationType}_validation.unknown_error`, {});
  }

  try {
    logger.debug(`Checking descriptions and notes are different between languages`);
    const duplicateDescriptionRows: TableData[] = [];
    const duplicateNoteRows: TableData[] = [];
    // const duplicateDescriptionRows = await quack.all(`
    //   SELECT "${joinColumn}", description, COUNT(language) as lang_count
    //   FROM (SELECT * FROM "${lookupTableName}" where description IS NOT NULL)
    //   GROUP BY description, "${joinColumn}" HAVING lang_count > 1
    // `);
    // const duplicateNoteRows = await quack.all(`
    //   SELECT "${joinColumn}", notes, COUNT(language) as lang_count
    //   FROM (SELECT * FROM "${lookupTableName}" WHERE notes IS NOT NULL)
    //   GROUP BY notes, "${joinColumn}" HAVING lang_count > 1
    // `);
    if (duplicateDescriptionRows.length > 0 || duplicateNoteRows.length > 0) {
      logger.error(`The lookup table has duplicate descriptions or notes`);
      // logger.error(`Duplicate descriptions: ${JSON.stringify(duplicateDescriptionRows)}`);
      // logger.error(`Duplicate notes: ${JSON.stringify(duplicateNoteRows)}`);
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
      `SELECT line_number, fact_table_column, "${lookupTableName}"."${joinColumn}" as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN "${lookupTableName}" ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST("${lookupTableName}"."${joinColumn}" AS VARCHAR)
            WHERE lookup_table_column IS NULL;`
    );
    logger.debug(`Number of rows from non matched rows query: ${nonMatchedRows.length}`);
    const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
    if (nonMatchedRows.length === rows[0].total_rows) {
      logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
      const nonMatchedFactTableValues = await quack.all(`SELECT DISTINCT "${factTableColumn}" FROM ${factTableName};`);
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

async function checkDecimalColumn(
  quack: Database,
  extractor: MeasureLookupTableExtractor,
  lookupTableName: string
): Promise<string[]> {
  const unmatchedFormats: string[] = [];
  logger.debug('Decimal column is present. Validating contains only positive integers.');
  const formats = await quack.all(`SELECT decimals FROM ${lookupTableName};`);
  for (const format of Object.values(formats.map((format) => format.decimals))) {
    if (format < 0) unmatchedFormats.push(format);
  }
  return unmatchedFormats;
}

async function checkFormatColumn(
  quack: Database,
  extractor: MeasureLookupTableExtractor,
  lookupTableName: string
): Promise<string[]> {
  const unmatchedFormats: string[] = [];
  logger.debug('Format column is present. Validating it contains only known formats.');
  const formats = await quack.all(`SELECT DISTINCT format FROM ${lookupTableName};`);
  // logger.debug(`Formats = ${JSON.stringify(Object.values(DataValueFormat), null, 2)}`);
  for (const format of Object.values(formats.map((format) => format.format))) {
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
  if (extractor.formatColumn && extractor.formatColumn.toLowerCase().includes('format')) {
    logger.debug('Formats column is present. Validating all formats present are valid.');
    const unMatchedFormats = await checkFormatColumn(quack, extractor, lookupTableName);
    if (unMatchedFormats.length > 0) {
      logger.debug(`Found invalid formats while validating format column`);
      return viewErrorGenerators(400, datasetId, 'patch', 'errors.measure_validation.invalid_formats_present', {
        totalNonMatching: unMatchedFormats.length,
        nonMatchingValues: unMatchedFormats,
        mismatch: false
      });
    }
  }

  if (extractor.decimalColumn && extractor.decimalColumn.toLowerCase().includes('decimal')) {
    const unmatchedDecimals = await checkDecimalColumn(quack, extractor, lookupTableName);
    if (unmatchedDecimals.length > 0) {
      logger.debug(`Found invalid formats while validating decimals column`);
      return viewErrorGenerators(400, datasetId, 'patch', 'errors.measure_validation.invalid_decimals_present', {
        totalNonMatching: unmatchedDecimals.length,
        nonMatchingValues: unmatchedDecimals,
        mismatch: false
      });
    }
  }
  logger.debug('Validating column contents complete.');
  return undefined;
};
