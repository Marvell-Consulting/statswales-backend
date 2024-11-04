import { IsString, IsUrl } from 'class-validator';

export class RelatedLinkDTO {
    @IsUrl()
    url: string;

    @IsString()
    label: string;
}
