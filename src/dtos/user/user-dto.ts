import { User } from '../../entities/user/user';
import { UserGroupRole } from '../../entities/user/user-group-role';
import { GroupRole } from '../../enums/group-role';
import { Locale } from '../../enums/locale';
import { UserGroupDTO } from './user-group-dto';
import { UserGroupWithRolesDTO } from './user-group-with-roles-dto';
import { UserStatus } from '../../enums/user-status';
import { GlobalRole } from '../../enums/global-role';

export class UserDTO {
  id: string;
  provider: string;
  provider_user_id?: string;
  email: string;
  given_name?: string;
  family_name?: string;
  full_name?: string;
  global_roles: GlobalRole[];
  groups: UserGroupWithRolesDTO[];
  status: UserStatus;
  created_at: Date;
  updated_at: Date;
  last_login_at: Date;

  static fromUser(user: User, lang: Locale): UserDTO {
    const dto = new UserDTO();

    dto.id = user.id;
    dto.provider = user.provider;
    dto.provider_user_id = user.providerUserId;
    dto.email = user.email;
    dto.given_name = user.givenName;
    dto.family_name = user.familyName;
    dto.full_name = user.name;
    dto.status = user.status;
    dto.created_at = user.createdAt;
    dto.updated_at = user.updatedAt;
    dto.last_login_at = user.lastLoginAt;

    dto.global_roles = user.globalRoles?.map((role) => role as GlobalRole) || [];

    dto.groups = user.groupRoles?.map((userRole: UserGroupRole) => {
      return {
        group: UserGroupDTO.fromUserGroup(userRole.group, lang),
        roles: userRole.roles.map((role) => role as GroupRole)
      };
    });

    return dto;
  }
}
