import { IsString, IsUrl, IsUUID } from 'class-validator';

export class RelatedLinkDTO {
    @IsUUID(4)
    id: string;

    @IsUrl()
    url: string;

    @IsString()
    label: string;
}
