import { IsArray, IsBoolean, IsEnum, IsNotEmpty, IsOptional, IsString, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';

import { DatasetInfo } from '../entities/dataset/dataset-info';
import { Designation } from '../enums/designation';

import { RelatedLinkDTO } from './related-link-dto';
import { UpdateFrequencyDTO } from './update-frequency-dto';

export class DatasetInfoDTO {
    @IsString()
    @IsNotEmpty()
    language: string;

    @IsString()
    @IsOptional()
    title?: string;

    @IsString()
    @IsOptional()
    description?: string;

    @IsString()
    @IsOptional()
    collection?: string;

    @IsString()
    @IsOptional()
    quality?: string;

    @IsBoolean()
    @IsOptional()
    rounding_applied?: boolean;

    @IsString()
    @IsOptional()
    rounding_description?: string;

    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => RelatedLinkDTO)
    @IsOptional()
    related_links?: RelatedLinkDTO[];

    @ValidateNested()
    @Type(() => UpdateFrequencyDTO)
    @IsOptional()
    update_frequency?: UpdateFrequencyDTO;

    @IsEnum(Designation)
    @IsOptional()
    designation?: Designation;

    static fromDatasetInfo(datasetInfo: DatasetInfo): DatasetInfoDTO {
        const dto = new DatasetInfoDTO();
        dto.language = datasetInfo.language;
        dto.title = datasetInfo.title;
        dto.description = datasetInfo.description;
        dto.collection = datasetInfo.collection;
        dto.quality = datasetInfo.quality;
        dto.rounding_applied = datasetInfo.roundingApplied;
        dto.rounding_description = datasetInfo.roundingDescription;
        dto.related_links = datasetInfo.relatedLinks;
        dto.update_frequency = UpdateFrequencyDTO.fromDuration(datasetInfo.updateFrequency);
        dto.designation = datasetInfo.designation;

        return dto;
    }

    static toDatasetInfo(dto: DatasetInfoDTO): Partial<DatasetInfo> {
        const datasetInfo = new DatasetInfo();
        datasetInfo.language = dto.language;
        datasetInfo.title = dto.title;
        datasetInfo.description = dto.description;
        datasetInfo.collection = dto.collection;
        datasetInfo.quality = dto.quality;
        datasetInfo.roundingApplied = dto.rounding_applied;
        datasetInfo.roundingDescription = dto.rounding_description;
        datasetInfo.relatedLinks = dto.related_links;
        datasetInfo.updateFrequency = UpdateFrequencyDTO.toDuration(dto.update_frequency);
        datasetInfo.designation = dto.designation;

        return datasetInfo;
    }
}
