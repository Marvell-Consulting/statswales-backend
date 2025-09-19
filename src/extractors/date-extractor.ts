import { YearType } from '../enums/year-type';

export interface DateExtractor {
  type: YearType;
  yearFormat?: string;
  quarterFormat?: string | null;
  quarterTotalIsFifthQuart?: boolean;
  monthFormat?: string;
  dateFormat?: string;
  startDay?: number;
  startMonth?: number;
  datasetStart?: Date;
  datasetEnd?: Date;
}
