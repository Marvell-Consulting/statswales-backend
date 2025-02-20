import { IsDateString, IsString, IsUrl } from 'class-validator';

export class RelatedLinkDTO {
    @IsString()
    id: string;

    @IsUrl()
    url: string;

    @IsString()
    label: string;

    @IsString()
    language: string;

    @IsString()
    @IsDateString()
    created_at: string;
}
