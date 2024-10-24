import { DimensionInfo } from '../entities/dataset/dimension-info';
import { Dimension } from '../entities/dataset/dimension';
import { Source } from '../entities/dataset/source';

import { SourceDTO } from './source-dto';
import { DimensionInfoDTO } from './dimension-info-dto';

export class DimensionDTO {
    id: string;
    type: string;
    start_revision_id: string;
    finish_revision_id?: string;
    validator?: string;
    sources?: SourceDTO[];
    dimensionInfo?: DimensionInfoDTO[];
    dataset_id?: string;

    static fromDimension(dimension: Dimension): DimensionDTO {
        const dimDto = new DimensionDTO();
        dimDto.id = dimension.id;
        dimDto.type = dimension.type;
        dimDto.start_revision_id = dimension.startRevision?.id;
        dimDto.finish_revision_id = dimension.finishRevision?.id;
        dimDto.validator = dimension.validator;

        dimDto.dimensionInfo = dimension.dimensionInfo?.map((dimInfo: DimensionInfo) => {
            return DimensionInfoDTO.fromDimensionInfo(dimInfo);
        });

        dimDto.sources = dimension.sources?.map((source: Source) => SourceDTO.fromSource(source));

        return dimDto;
    }
}
