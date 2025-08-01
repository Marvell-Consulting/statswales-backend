import { Organisation } from '../entities/user/organisation';
import { Locale } from '../enums/locale';

export class OrganisationDTO {
  id: string;
  name?: string;

  static fromOrganisation(org: Organisation, lang: Locale): OrganisationDTO {
    const meta = org.metadata?.find((m) => m.language.includes(lang));
    const dto = new OrganisationDTO();
    dto.id = org.id;
    dto.name = meta?.name;
    return dto;
  }
}
