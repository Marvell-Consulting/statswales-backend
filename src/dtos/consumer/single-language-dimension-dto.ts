import { DimensionMetadataDTO } from '../dimension-metadata-dto';
import { Dimension } from '../../entities/dataset/dimension';

export class SingleLanguageDimensionDTO {
  id: string;
  fact_table_column: string;
  metadata?: DimensionMetadataDTO;

  static fromDimension(dimension: Dimension, lang: string): SingleLanguageDimensionDTO {
    const dto = new SingleLanguageDimensionDTO();
    dto.id = dimension.id;
    dto.fact_table_column = dimension.factTableColumn;
    dto.metadata = dimension.metadata?.find((meta) => meta.language === lang);

    return dto;
  }
}
