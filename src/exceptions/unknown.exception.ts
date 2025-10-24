import { CubeBuildResult } from '../dtos/cube-build-result';

export class UnknownException extends Error {
  performance: CubeBuildResult | null = null;
  constructor(
    public message = 'Server Error',
    public status = 500
  ) {
    super(message);
    this.name = 'UnknownException';
    this.status = status;
  }
}
