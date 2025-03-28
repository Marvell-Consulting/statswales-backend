import { DeepPartial } from 'typeorm';
import { UserGroup } from '../../entities/user/user-group';
import { Locale } from '../../enums/locale';
import { DatasetDTO } from '../dataset-dto';

import { OrganisationDTO } from '../organisation-dto';
import { UserDto } from './user-dto';
import { UserGroupMetadataDTO } from './user-group-metadata-dto';

export class UserGroupDTO {
  id: string;
  prefix?: string;
  metadata?: UserGroupMetadataDTO[];
  organisation_id?: string;
  organisation?: OrganisationDTO;
  users: UserDto[];
  user_count?: number;
  datasets: DatasetDTO[];
  dataset_count?: number;

  static fromUserGroup(userGroup: UserGroup, lang: Locale): UserGroupDTO {
    const dto = new UserGroupDTO();

    dto.id = userGroup.id;
    dto.organisation_id = userGroup.organisation?.id;
    dto.organisation = userGroup.organisation
      ? OrganisationDTO.fromOrganisation(userGroup.organisation, lang)
      : undefined;

    dto.metadata = userGroup.metadata?.map((meta) => UserGroupMetadataDTO.fromUserGroupMetadata(meta));

    return dto;
  }

  static toUserGroup(dto: UserGroupDTO): DeepPartial<UserGroup> {
    return UserGroup.create({
      id: dto.id,
      prefix: dto.prefix,
      organisationId: dto.organisation_id,
      metadata: dto.metadata?.map((m) => UserGroupMetadataDTO.toUserGroupMetadata(m))
    });
  }
}
