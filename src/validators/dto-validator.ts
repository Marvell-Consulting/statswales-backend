import { plainToInstance } from 'class-transformer';
import { validate, ValidationError } from 'class-validator';

import { BadRequestException } from '../exceptions/bad-request.exception';

// converts a plain object (e.g. req.body json) to an instance of the expected DTO and validates it
export const dtoValidator = async (expectedDTO: any, requestObject: object) => {
    const dto: any = plainToInstance(expectedDTO, requestObject);
    const errors = await validate(dto);

    if (errors.length > 0) {
        throw new BadRequestException('errors.invalid_dto', 400, errors);
    }

    return dto;
};

export const arrayValidator = async (expectedDTO: any, value: any[]) => {
    let errors: ValidationError[] = [];
    const dtos = [];

    for (const item of value) {
        const dto: any = plainToInstance(expectedDTO, item);
        dtos.push(dto);

        const itemErrors = await validate(dto);

        if (itemErrors.length > 0) {
            errors = errors.concat(itemErrors);
        }
    }

    if (errors.length > 0) {
        throw new BadRequestException('errors.invalid_dto', 400, errors);
    }

    return dtos;
};
