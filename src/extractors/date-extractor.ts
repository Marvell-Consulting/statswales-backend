import { YearType } from '../enums/year-type';

export interface DateExtractor {
    type?: YearType;
    yearFormat?: string;
    quarterFormat?: string;
    quarterTotalIsFifthQuart?: boolean;
    monthFormat?: string;
    dateFormat?: string;
}
