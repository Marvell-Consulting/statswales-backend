import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionType } from '../enums/dimension-type';

import { DimensionMetadataDTO } from './dimension-metadata-dto';
import { LookupTableDTO } from './lookup-table-dto';

export class DimensionDTO {
  id: string;
  dataset_id: string;
  type: DimensionType;
  extractor?: object;
  joinColumn?: string; // <-- Tells you have to join the dimension to the fact_table
  factTableColumn: string; // <-- Tells you which column in the fact table you're joining to
  isSliceDimension: boolean;
  lookupTable?: LookupTableDTO;
  metadata?: DimensionMetadataDTO[];

  static fromDimension(dimension: Dimension): DimensionDTO {
    const dimDto = new DimensionDTO();
    dimDto.id = dimension.id;
    dimDto.type = dimension.type;
    dimDto.extractor = dimension.extractor || undefined;
    dimDto.lookupTable = dimension?.lookupTable ? LookupTableDTO.fromLookupTable(dimension?.lookupTable) : undefined;
    dimDto.joinColumn = dimension.joinColumn || undefined;
    dimDto.factTableColumn = dimension.factTableColumn;
    dimDto.isSliceDimension = dimension.isSliceDimension;

    dimDto.metadata = dimension.metadata?.map((dimInfo: DimensionMetadata) => {
      return DimensionMetadataDTO.fromDimensionMetadata(dimInfo);
    });

    return dimDto;
  }
}
