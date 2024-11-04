import { IsBoolean, IsEnum, IsNotEmpty, IsOptional, IsString, ValidateNested } from 'class-validator';

import { IsISO8601Duration } from '../validators/is-iso8601-duration';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { Designation } from '../enums/designation';

import { RelatedLinkDTO } from './related-link-dto';

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

    @ValidateNested()
    @IsOptional()
    related_links?: RelatedLinkDTO[];

    @IsISO8601Duration()
    @IsOptional()
    update_frequency?: string; // in ISO 8601 duration format, e.g. P1Y = every year

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
        dto.update_frequency = datasetInfo.updateFrequency;
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
        datasetInfo.updateFrequency = dto.update_frequency;
        datasetInfo.designation = dto.designation;

        return datasetInfo;
    }
}
