import { DeepPartial } from 'typeorm';
import { UserGroup } from '../../entities/user/user-group';
import { Locale } from '../../enums/locale';
import { DatasetDTO } from '../dataset-dto';

import { OrganisationDTO } from '../organisation-dto';
import { UserDto } from './user-dto';

export class UserGroupDTO {
  id: string;
  prefix?: string;
  name?: string;
  email?: string;
  organisation_id?: string;
  organisation?: OrganisationDTO;
  users: UserDto[];
  user_count?: number;
  datasets: DatasetDTO[];
  dataset_count?: number;

  static fromUserGroup(userGroup: UserGroup, lang: Locale): UserGroupDTO {
    const meta = userGroup.metadata?.find((meta) => lang.includes(meta.language));

    const dto = new UserGroupDTO();
    dto.id = userGroup.id;
    dto.name = meta?.name;
    dto.email = meta?.email;
    dto.organisation_id = userGroup.organisation?.id;
    dto.organisation = userGroup.organisation
      ? OrganisationDTO.fromOrganisation(userGroup.organisation, lang)
      : undefined;

    return dto;
  }

  static toUserGroup(dto: UserGroupDTO): DeepPartial<UserGroup> {
    return {
      prefix: dto.prefix,
      organisationId: dto.organisation_id,
      metadata: [
        {
          name: dto.name,
          email: dto.email,
          id: '',
          language: ''
        }
      ]
    };
  }
}
