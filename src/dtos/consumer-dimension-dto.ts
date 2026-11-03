import { Dimension } from '../entities/dataset/dimension';
import { DimensionType } from '../enums/dimension-type';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';

export interface ConsumerDimensionMetadataDTO {
  language: string;
  name: string;
}

// The cube's filter_table stores languages in lower case ('en-gb'); the API has always returned 'en-GB'.
const toLocaleCase = (language: string): string => {
  const [lang, region] = language.split('-');
  return region ? `${lang.toLowerCase()}-${region.toUpperCase()}` : lang.toLowerCase();
};

// Dimension records are shared with any draft update of the dataset, so the names must come from
// the published revision's cube rather than the dimension's own metadata, and fields that only the
// cube builder needs (extractor, lookup table, join column) must not be exposed.
export class ConsumerDimensionDTO {
  id: string;
  type: DimensionType;
  factTableColumn: string;
  isSliceDimension: boolean;
  metadata?: ConsumerDimensionMetadataDTO[];

  static fromDimension(dimension: Dimension, publishedNames?: FactTableToDimensionName[]): ConsumerDimensionDTO {
    const dto = new ConsumerDimensionDTO();
    dto.id = dimension.id;
    dto.type = dimension.type;
    dto.factTableColumn = dimension.factTableColumn;
    dto.isSliceDimension = dimension.isSliceDimension;
    dto.metadata = publishedNames
      ?.filter((row) => row.fact_table_column === dimension.factTableColumn)
      .map((row) => ({ language: toLocaleCase(row.language), name: row.dimension_name }));

    return dto;
  }
}
