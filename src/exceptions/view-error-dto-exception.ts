export class ViewErrorDtoException extends Error {
  status: number;
  message: string;
  error?: Error | unknown;
  dataLength?: number;
  nonMatchingValues?: unknown[];

  constructor(
    status: number,
    message: string,
    error?: Error | unknown,
    dataLength?: number,
    nonMatchingValues?: unknown[]
  ) {
    super(message);
    this.status = status;
    this.error = error;
    this.dataLength = dataLength;
    this.nonMatchingValues = nonMatchingValues;
  }
}
