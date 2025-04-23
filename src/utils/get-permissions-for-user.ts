import { pick } from 'lodash';
import { User } from '../entities/user/user';
import { UserDTO } from '../dtos/user/user-dto';
import { UserGroupRole } from '../entities/user/user-group-role';
import { UserGroupWithRolesDTO } from '../dtos/user/user-group-with-roles-dto';

export const getPermissionsForUserDTO = (user: UserDTO) => {
  return {
    ...pick(user, ['id', 'global_roles', 'status']),
    groups: user.groups.map((ugWithRoles: UserGroupWithRolesDTO) => ({
      id: ugWithRoles.group.id,
      roles: ugWithRoles.roles
    }))
  };
};

export const getPermissionsForUser = (user: User) => {
  return {
    ...pick(user, ['id', 'global_roles', 'status']),
    groups: user.groupRoles.map((groupRole: UserGroupRole) => ({
      id: groupRole.groupId,
      roles: groupRole.roles
    }))
  };
};

export const getUserGroupIdsForUser = (user: User) => {
  return user.groupRoles.map((groupRole: UserGroupRole) => groupRole.groupId);
};
