import { IsBoolean, IsEnum, IsInt, IsOptional } from 'class-validator';
import { invert } from 'lodash';

import { DurationUnit } from '../enums/duration-unit';

/* eslint-disable id-length */
const durationCodeToUnit = {
    D: DurationUnit.Day,
    W: DurationUnit.Week,
    M: DurationUnit.Month,
    Y: DurationUnit.Year
};
/* eslint-enable id-length */

const unitToDurationCode = invert(durationCodeToUnit);

export class UpdateFrequencyDTO {
    @IsBoolean()
    is_updated?: boolean;

    @IsInt()
    @IsOptional()
    frequency_value?: number;

    @IsEnum(DurationUnit)
    @IsOptional()
    frequency_unit?: string;

    static fromDuration(duration?: string): UpdateFrequencyDTO | undefined {
        if (!duration) return undefined;
        if (duration === 'NEVER') return { is_updated: false };
        const durationCode = duration.slice(-1) as keyof typeof durationCodeToUnit;

        return {
            is_updated: true,
            frequency_value: parseInt(duration.replace(/[^0-9]/, ''), 10),
            frequency_unit: durationCodeToUnit[durationCode]
        };
    }

    static toDuration(update: UpdateFrequencyDTO | undefined): string | undefined {
        if (!update) return undefined;
        if (!update.is_updated) return 'NEVER';
        return `P${update.frequency_value}${unitToDurationCode[update.frequency_unit!]}`;
    }
}
