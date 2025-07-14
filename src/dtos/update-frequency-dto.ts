import { IsEnum, IsOptional, IsString, ValidateNested } from 'class-validator';

import { UpdateType } from '../enums/update-frequency';
import { Type } from 'class-transformer';

export class UpdateDateDTO {
  @IsString()
  @IsOptional()
  day?: string;

  @IsString()
  month: string;

  @IsString()
  year: string;
}

export class UpdateFrequencyDTO {
  @IsEnum(UpdateType)
  update_type?: UpdateType;

  @ValidateNested()
  @Type(() => UpdateDateDTO)
  @IsOptional()
  date?: UpdateDateDTO;
}
