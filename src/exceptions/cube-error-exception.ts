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

export class CubeValidationException extends Error {
  public type: CubeValidationType;
  public revisionId: string;
  public datasetId: string;
  public originalError: string;
  public fact: unknown;
  constructor(public message: string) {
    super(message);
  }
}
