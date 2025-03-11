import { IsDateString, IsOptional, IsString, IsUrl } from 'class-validator';

export interface RelatedLink {
  id: string;
  url: string;
  labelEN?: string;
  labelCY?: string;
  created_at: string;
}

export class RelatedLinkDTO {
  @IsString()
  id: string;

  @IsUrl()
  url: string;

  @IsString()
  @IsOptional()
  label_en?: string;

  @IsString()
  @IsOptional()
  label_cy?: string;

  @IsString()
  @IsDateString()
  created_at: string;

  static fromRelatedLink(relLink: RelatedLink): RelatedLinkDTO {
    return {
      id: relLink.id,
      url: relLink.url,
      created_at: relLink.created_at,
      label_en: relLink.labelEN,
      label_cy: relLink.labelCY
    };
  }

  static toRelatedLink(dto: RelatedLinkDTO): RelatedLink {
    return {
      id: dto.id,
      url: dto.url,
      created_at: dto.created_at,
      labelEN: dto.label_en,
      labelCY: dto.label_cy
    };
  }
}
