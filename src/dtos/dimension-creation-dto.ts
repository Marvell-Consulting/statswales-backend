import { SourceType } from '../enums/source-type';

export interface DimensionCreationDTO {
    sourceId: string;
    sourceType: SourceType;
}
