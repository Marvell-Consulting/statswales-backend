import { Organisation } from '../entities/user/organisation';
import { Locale } from '../enums/locale';

export class OrganisationDTO {
    id: string;
    name?: string;

    static fromOrganisation(org: Organisation, lang: Locale): OrganisationDTO {
        const dto = new OrganisationDTO();
        dto.id = org.id;
        dto.name = lang.includes('en') ? org.nameEN : org.nameCY;
        return dto;
    }
}
