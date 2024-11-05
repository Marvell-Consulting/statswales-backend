import { validate } from 'class-validator';

import { IsISO8601Duration } from './is-iso8601-duration';

class ObjectWithDuration {
    @IsISO8601Duration()
    duration?: string;

    constructor(duration?: string) {
        this.duration = duration;
    }
}

const invalidDurations = ['', 'INVALID', 'P$1', 'P11', 'P0.5Y', 'P0.5M', 'P0.5D', 'PT0,2H0,1S'];

const validDurations = ['P1Y', 'P15M', 'P0D', 'PT0S', 'P2W2D', 'P1DT2H3M4S', 'P4Y2M8D'];

describe('isIso8601Duration', () => {
    it('should return errors for invalid duration strings', async () => {
        for (const duration of invalidDurations) {
            const testSubject = new ObjectWithDuration(duration);
            const errors = await validate(testSubject);
            expect(errors.length).toBe(1);
        }
    });

    it('should not return errors for valid duration strings', async () => {
        for (const duration of validDurations) {
            const testSubject = new ObjectWithDuration(duration);
            const errors = await validate(testSubject);
            expect(errors.length).toBe(0);
        }
    });
});
