export enum CubeValidationType {
  FactTable = 'factTable',
  FactTableColumnMissing = 'factTableColumnMissing',
  DuplicateFact = 'duplicateFact',
  Dimension = 'dimension',
  DimensionNonMatchedRows = 'dimension_non_matched_rows',
  Measure = 'measure',
  NoFirstRevision = 'no_first_revision',
  CubeCreationFailed = 'cube_creation_failed'
}
