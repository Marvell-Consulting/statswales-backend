import { CubeValidationType } from '../enums/cube-validation-type';

export class CubeValidationException extends Error {
  public type: CubeValidationType;
  public revisionId: string;
  public datasetId: string;
  public originalError?: unknown;
  public buildLog?: string[];

  constructor(
    public message: string,
    datasetId: string,
    revisionId: string,
    type: CubeValidationType
  ) {
    super(message);
    this.datasetId = datasetId;
    this.revisionId = revisionId;
    this.type = type;
  }
}
