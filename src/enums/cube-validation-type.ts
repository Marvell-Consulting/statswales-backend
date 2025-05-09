export enum CubeValidationType {
  FactTable = 'fact_table',
  NoFactTable = 'no_fact_table',
  FactTableCreateFailed = 'fact_table_create_failed',
  FactTableColumnMissing = 'fact_table_column_missing',
  UnknownErrLoadingFactTablesFailed = 'loading_fact_tables_failed',
  NoDataTables = 'no_data_tables',
  DataLakeError = 'data_lake_error',
  NoNotesCodeColumn = 'no_notes_code_column',
  DuplicateFact = 'duplicate_fact',
  UnknownDuplicateFact = 'unknown_duplicate_fact',
  Dimension = 'dimension',
  DimensionNonMatchedRows = 'dimension_non_matched_rows',
  Measure = 'measure',
  NoFirstRevision = 'no_first_revision',
  CubeCreationFailed = 'cube_creation_failed',
  UnknownFileType = 'unknown_file_type'
}
