import { DimensionInfo } from '../entities/dataset/dimension-info';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionType } from '../enums/dimension-type';

import { DimensionInfoDTO } from './dimension-info-dto';
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
    dimensionInfo?: DimensionInfoDTO[];

    static fromDimension(dimension: Dimension): DimensionDTO {
        const dimDto = new DimensionDTO();
        dimDto.id = dimension.id;
        dimDto.type = dimension.type;
        dimDto.extractor = dimension.extractor || undefined;
        dimDto.lookupTable = dimension?.lookupTable
            ? LookupTableDTO.fromLookupTable(dimension?.lookupTable)
            : undefined;
        dimDto.joinColumn = dimension.joinColumn || undefined;
        dimDto.factTableColumn = dimension.factTableColumn;
        dimDto.isSliceDimension = dimension.isSliceDimension;

        dimDto.dimensionInfo = dimension.dimensionInfo?.map((dimInfo: DimensionInfo) => {
            return DimensionInfoDTO.fromDimensionInfo(dimInfo);
        });
        return dimDto;
    }
}
