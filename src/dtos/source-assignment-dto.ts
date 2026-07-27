import { IsEnum, IsInt, IsString } from 'class-validator';

import { FactTableColumnType } from '../enums/fact-table-column-type';

export class SourceAssignmentDTO {
  @IsInt()
  column_index: number;

  @IsString()
  column_name: string;

  @IsEnum(FactTableColumnType)
  column_type: FactTableColumnType;
}
