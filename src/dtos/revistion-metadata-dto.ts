import { IsArray, IsBoolean, IsEnum, IsNotEmpty, IsOptional, IsString, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';

import { Revision } from '../entities/dataset/revision';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { Designation } from '../enums/designation';

import { RelatedLinkDTO } from './related-link-dto';
import { UpdateFrequencyDTO } from './update-frequency-dto';

export interface SplitMeta {
    revision: Partial<Revision>;
    metadata: Partial<RevisionMetadata>;
}

export class RevisionMetadataDTO {
    @IsString()
    @IsNotEmpty()
    language: string;

    @IsString()
    @IsOptional()
    title?: string;

    @IsString()
    @IsOptional()
    summary?: string;

    @IsString()
    @IsOptional()
    collection?: string;

    @IsString()
    @IsOptional()
    quality?: string;

    @IsBoolean()
    @IsOptional()
    rounding_applied?: boolean; // this is a prop of revision but required when patching rounding_description

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

    static fromRevisionMetadata(meta: RevisionMetadata): RevisionMetadataDTO {
        const dto = new RevisionMetadataDTO();
        dto.language = meta.language;
        dto.title = meta.title;
        dto.summary = meta.summary;
        dto.collection = meta.collection;
        dto.quality = meta.quality;
        dto.rounding_description = meta.roundingDescription;

        return dto;
    }

    static splitMeta(dto: RevisionMetadataDTO): SplitMeta {
        const metadata: Partial<RevisionMetadata> = {
            language: dto.language,
            title: dto.title,
            summary: dto.summary,
            collection: dto.collection,
            quality: dto.quality,
            roundingDescription: dto.rounding_description
        };

        const revision: Partial<Revision> = {
            roundingApplied: dto.rounding_applied,
            relatedLinks: dto.related_links?.map((linkDto) => RelatedLinkDTO.toRelatedLink(linkDto)),
            designation: dto.designation,
            updateFrequency: UpdateFrequencyDTO.toDuration(dto.update_frequency)
        };

        return { metadata, revision };
    }
}
