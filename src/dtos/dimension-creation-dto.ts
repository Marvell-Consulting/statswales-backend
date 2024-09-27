import { SourceType } from '../enums/source-type.enum';

export interface DimensionCreationDTO {
    sourceId: string;
    sourceType: SourceType;
}
