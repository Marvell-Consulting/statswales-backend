import { DimensionType } from '../enums/dimension-type';
import { YearType } from '../enums/year-type';
import { ReferenceType } from '../enums/reference-type';
import { NumberType } from '../extractors/number-extractor';

export interface DimensionPatchDto {
  dimension_type: DimensionType;
  dimension_title?: string;
  lookup_join_column?: string;
  reference_type?: ReferenceType;
  date_type?: YearType;
  year_format?: string;
  quarter_format?: string;
  month_format?: string;
  date_format?: string;
  fifth_quarter: boolean;
  number_format?: NumberType;
  decimal_places?: number;
}
