import { registerDecorator, ValidationOptions } from 'class-validator';

export const IsISO8601Duration = (validationOptions?: ValidationOptions) => {
    // eslint-disable-next-line @typescript-eslint/ban-types
    return (object: Object, propertyName: string) => {
        registerDecorator({
            name: 'isISO8601Duration',
            target: object.constructor,
            propertyName,
            options: validationOptions,
            constraints: [],
            validator: {
                validate(value: any) {
                    return /^P(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+S)?)?$/.test(value);
                },
                defaultMessage() {
                    return `$property must be a valid ISO 8601 duration string`;
                }
            }
        });
    };
};
