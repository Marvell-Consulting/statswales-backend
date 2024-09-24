import { DimensionInfo } from '../entities/dimension-info';
import { Dimension } from '../entities/dimension';
import { Source } from '../entities/source';

import { SourceDTO } from './source-dto';

export class DimensionInfoDTO {
    language?: string;
    name: string;
    description?: string;
    notes?: string;

    static fromDimensionInfo(dimensionInfo: DimensionInfo): DimensionInfoDTO {
        const dto = new DimensionInfoDTO();
        dto.language = dimensionInfo.language;
        dto.name = dimensionInfo.name;
        dto.description = dimensionInfo.description;
        dto.notes = dimensionInfo.notes;
        return dto;
    }
}

export class DimensionDTO {
    id: string;
    type: string;
    start_revision_id: string;
    finish_revision_id?: string;
    validator?: string;
    sources?: SourceDTO[];
    dimensionInfo?: DimensionInfoDTO[];
    dataset_id?: string;

    static async fromDimension(dimension: Dimension): Promise<DimensionDTO> {
        const dimDto = new DimensionDTO();
        dimDto.id = dimension.id;
        dimDto.type = dimension.type;
        dimDto.start_revision_id = (await dimension.startRevision).id;
        dimDto.finish_revision_id = (await dimension.finishRevision)?.id;
        dimDto.validator = dimension.validator;
        dimDto.dimensionInfo = (await dimension.dimensionInfo).map((dimInfo: DimensionInfo) => {
            const infoDto = DimensionInfoDTO.fromDimensionInfo(dimInfo);
            return infoDto;
        });
        dimDto.sources = [];
        return dimDto;
    }

    static async fromDimensionWithSources(dimension: Dimension): Promise<DimensionDTO> {
        const dimDto = await DimensionDTO.fromDimension(dimension);
        dimDto.sources = await Promise.all(
            (await dimension.sources).map(async (source: Source) => {
                const sourceDto = await SourceDTO.fromSource(source);
                return sourceDto;
            })
        );
        return dimDto;
    }
}
