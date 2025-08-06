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
  name?: string;
  global_roles: GlobalRole[];
  groups: UserGroupWithRolesDTO[];
  status: UserStatus;
  created_at?: string;
  updated_at?: string;
  last_login_at?: string;

  static fromUser(user: User, lang: Locale): UserDTO {
    const dto = new UserDTO();

    dto.id = user.id;
    dto.provider = user.provider;
    dto.provider_user_id = user.providerUserId;
    dto.email = user.email;
    dto.name = user.name;
    dto.status = user.status;
    dto.created_at = user.createdAt?.toISOString();
    dto.updated_at = user.updatedAt?.toISOString();
    dto.last_login_at = user.lastLoginAt?.toISOString();

    dto.global_roles = user.globalRoles?.map((role) => role as GlobalRole) || [];

    dto.groups = (user.groupRoles || []).map((userRole: UserGroupRole) => {
      return {
        group: UserGroupDTO.fromUserGroup(userRole.group, lang),
        roles: userRole.roles.map((role) => role as GroupRole)
      };
    });

    return dto;
  }

  // Create a minimal UserDTO for JWT purposes to keep the size down
  static fromUserForJWT(user: User): UserDTO {
    const dto = new UserDTO();

    dto.id = user.id;
    dto.email = user.email;
    dto.name = user.name;
    dto.status = user.status;

    dto.global_roles = user.globalRoles?.map((role) => role as GlobalRole) || [];

    dto.groups = (user.groupRoles || []).map((userRole: UserGroupRole) => {
      return {
        group: { id: userRole.groupId },
        roles: userRole.roles.map((role) => role as GroupRole)
      };
    });

    return dto;
  }
}
