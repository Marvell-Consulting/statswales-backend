import { SourceType } from '../entities/source_type';

export interface DimensionCreationDTO {
    sourceId: string;
    sourceType: SourceType;
}
