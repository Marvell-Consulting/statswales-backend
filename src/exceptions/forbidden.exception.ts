export class ForbiddenException extends Error {
  constructor(
    public message = 'Permission Denied',
    public status = 403
  ) {
    super(message);
    this.name = 'ForbiddenException';
    this.status = status;
  }
}
