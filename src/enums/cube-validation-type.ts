export enum CubeValidationType {
  FactTable = 'fact_table',
  FactTableColumnMissing = 'fact_table_column_missing',
  DuplicateFact = 'duplicate_fact',
  UnknownDuplicateFact = 'unknown_duplicate_fact',
  Dimension = 'dimension',
  DimensionNonMatchedRows = 'dimension_non_matched_rows',
  Measure = 'measure',
  NoFirstRevision = 'no_first_revision',
  CubeCreationFailed = 'cube_creation_failed',
  UnknownFileType = 'unknown_file_type'
}
