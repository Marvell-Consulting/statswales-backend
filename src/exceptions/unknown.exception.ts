export class UnknownException extends Error {
    constructor(
        public message = 'Server Error',
        public status = 500
    ) {
        super(message);
        this.name = 'UnknownException';
        this.status = status;
    }
}
