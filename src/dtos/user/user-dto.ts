import { User } from '../../entities/user/user';
import { Locale } from '../../enums/locale';
import { UserGroupDTO } from './user-group-dto';

export class UserDto {
  id: string;
  provider: string;
  email: string;
  given_name?: string;
  family_name?: string;
  groups: UserGroupDTO[];
  created_at: Date;
  updated_at: Date;

  static fromUser(user: User, lang: Locale): UserDto {
    const dto = new UserDto();

    dto.id = user.id;
    dto.provider = user.provider;
    dto.email = user.email;
    dto.given_name = user.givenName;
    dto.family_name = user.familyName;
    dto.created_at = user.createdAt;
    dto.updated_at = user.updatedAt;

    dto.groups = user.groups?.map((group) => UserGroupDTO.fromUserGroup(group, lang));

    return dto;
  }
}
