export class BadRequestException extends Error {
    constructor(
        public message = 'Bad Request',
        public status = 400
    ) {
        super(message);
        this.name = 'BadRequestException';
        this.status = status;
    }
}
