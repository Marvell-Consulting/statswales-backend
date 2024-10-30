import { ValidationError } from 'class-validator';

export class BadRequestException extends Error {
    constructor(
        public message = 'Bad Request',
        public status = 400,
        public validationErrors?: ValidationError[]
    ) {
        super(message);
        this.name = 'BadRequestException';
        this.status = status;
        this.validationErrors = validationErrors;
    }
}
