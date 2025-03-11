export enum CubeValidationType {
  FactTable = 'factTable',
  DuplicateFact = 'duplicateFact',
  Dimension = 'dimension',
  DimensionNonMatchedRows = 'dimension_non_matched_rows',
  Measure = 'measure'
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
