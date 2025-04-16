import { pick } from 'lodash';
import { User } from '../entities/user/user';
import { UserDTO } from '../dtos/user/user-dto';
import { UserGroupRole } from '../entities/user/user-group-role';
import { UserGroupWithRolesDTO } from '../dtos/user/user-group-with-roles-dto';

export const getPermissionsForUserDTO = (user: UserDTO) => {
  return {
    ...pick(user, ['id', 'global_roles', 'status']),
    groups: user.groups.map((group: UserGroupWithRolesDTO) => pick(group, 'id', 'roles'))
  };
};

export const getPermissionsForUser = (user: User) => {
  return {
    ...pick(user, ['id', 'global_roles', 'status']),
    groups: user.groupRoles.map((group: UserGroupRole) => ({
      id: group.groupId,
      roles: group.roles
    }))
  };
};

export const getUserGroupIdsForUser = (user: User) => {
  return user.groupRoles.map((group: UserGroupRole) => group.groupId);
};
