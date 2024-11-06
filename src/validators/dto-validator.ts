import util from 'node:util';

import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { ValidationError } from 'express-validator';

import { BadRequestException } from '../exceptions/bad-request.exception';

// converts a plain object (e.g. req.body json) to an instance of the expected DTO and validates it
export const dtoValidator = async (expectedDTO: any, requestObject: object) => {
    const dto: any = plainToInstance(expectedDTO, requestObject);
    const errors = await validate(dto);

    console.log(util.inspect(errors, false, null, true));

    if (errors.length > 0) {
        throw new BadRequestException('errors.invalid_dto', 400, errors);
    }

    return dto;
};

// export const flattenValidationErrors = (errors: ValidationError[]): string[] => {
//     return errors
//         .map((error) => {
//             if (error.children && error.children.length > 0) {
//                 return flattenValidationErrors(error.children);
//             }

//             return Object.values(error.constraints);
//         })
//         .flat();
// };
