import { IsOptional, IsString } from 'class-validator';

import { MetadataInterface } from '../interfaces/metadata-interface';

export class DimensionMetadataDTO {
  @IsString()
  language: string;

  @IsString()
  @IsOptional()
  name: string;

  @IsString()
  @IsOptional()
  description?: string;

  @IsString()
  @IsOptional()
  notes?: string;

  static fromDimensionMetadata(dimensionInfo: MetadataInterface): DimensionMetadataDTO {
    const dto = new DimensionMetadataDTO();
    dto.language = dimensionInfo.language;
    dto.name = dimensionInfo.name;
    dto.description = dimensionInfo.description;
    dto.notes = dimensionInfo.notes;

    return dto;
  }
}
