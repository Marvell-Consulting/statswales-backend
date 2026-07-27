import { IsEnum, IsInt, IsNotEmpty, IsString } from 'class-validator';

import { FactTableColumnType } from '../enums/fact-table-column-type';

export class SourceAssignmentDTO {
  @IsInt()
  @IsNotEmpty()
  column_index: number;

  @IsString()
  @IsNotEmpty()
  column_name: string;

  @IsEnum(FactTableColumnType)
  @IsNotEmpty()
  column_type: FactTableColumnType;
}
