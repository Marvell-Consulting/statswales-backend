export class SourceAssignmentException extends Error {
  constructor(
    public message: string,
    public status = 400
  ) {
    super(message);
    this.name = 'SourceAssignmentException';
    this.status = status;
  }
}
