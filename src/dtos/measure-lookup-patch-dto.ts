import { IsArray, IsOptional, IsString } from 'class-validator';

export class MeasureLookupPatchDTO {
  @IsOptional()
  @IsString()
  join_column?: string;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  description_columns?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  notes_columns?: string[];

  @IsOptional()
  @IsString()
  sort_column?: string;

  @IsOptional()
  @IsString()
  measure_type_column?: string;

  @IsOptional()
  @IsString()
  format_column?: string;

  @IsOptional()
  @IsString()
  decimal_column?: string;

  @IsOptional()
  @IsString()
  language_column?: string;
}
