import { pick } from 'lodash';
import { UserDTO } from '../dtos/user/user-dto';
import { UserGroupWithRolesDTO } from '../dtos/user/user-group-with-roles-dto';

export const getPermissionsForUser = (user: UserDTO) => {
  return {
    ...pick(user, ['id', 'global_roles', 'status']),
    groups: user.groups.map((group: UserGroupWithRolesDTO) => pick(group, 'id', 'roles'))
  };
};
