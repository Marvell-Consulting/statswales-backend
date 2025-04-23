export class NotAllowedException extends Error {
  constructor(
    public message = 'Not Allowed',
    public status = 405
  ) {
    super(message);
    this.name = 'NotAllowedException';
    this.status = status;
  }
}
