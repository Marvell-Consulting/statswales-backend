import { CubeValidationType } from '../enums/cube-validation-type';

export class CubeValidationException extends Error {
  public type: CubeValidationType;
  public revisionId: string;
  public datasetId: string;
  public originalError: string;
  public fact: unknown;
  public status = 400;
  constructor(public message: string) {
    super(message);
  }
}
