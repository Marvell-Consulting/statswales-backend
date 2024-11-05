import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';

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
