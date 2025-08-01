import { UserGroup } from '../entities/user/user-group';
import { OrganisationDTO } from './organisation-dto';
import { Locale } from '../enums/locale';
import { UserGroupListItemDTO } from './user/user-group-list-item-dto';

export class PublisherDTO {
  group: UserGroupListItemDTO;
  organisation: OrganisationDTO;

  static fromUserGroup(userGroup: UserGroup, lang: Locale): PublisherDTO {
    const dto = new PublisherDTO();
    dto.group = {
      id: userGroup.id,
      name: userGroup.metadata?.find((m) => m.language.includes(lang))?.name || '',
      email: userGroup.metadata?.find((m) => m.language.includes(lang))?.email || ''
    };

    if (userGroup.organisation) {
      dto.organisation = OrganisationDTO.fromOrganisation(userGroup.organisation, lang);
    }

    return dto;
  }
}
