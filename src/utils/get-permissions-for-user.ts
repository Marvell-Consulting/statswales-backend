import { pick } from 'lodash';
import { User } from '../entities/user/user';
import { UserDTO } from '../dtos/user/user-dto';
import { UserGroupRole } from '../entities/user/user-group-role';
import { UserGroupWithRolesDTO } from '../dtos/user/user-group-with-roles-dto';
import { GroupRole } from '../enums/group-role';
import { Dataset } from '../entities/dataset/dataset';

interface PermissionsForUserDTO {
  id: string;
  global_roles: string[];
  status: string;
  groups: {
    id: string;
    roles: string[];
  }[];
}

export const getPermissionsForUserDTO = (user: UserDTO): PermissionsForUserDTO => {
  return {
    ...pick(user, ['id', 'global_roles', 'status']),
    groups: user.groups.map((ugWithRoles: UserGroupWithRolesDTO) => ({
      id: ugWithRoles.group.id,
      roles: ugWithRoles.roles
    }))
  };
};

export const getPermissionsForUser = (user: User): PermissionsForUserDTO => {
  return {
    id: user.id,
    global_roles: user.globalRoles,
    status: user.status,
    groups: user.groupRoles.map((groupRole: UserGroupRole) => ({
      id: groupRole.groupId,
      roles: groupRole.roles
    }))
  };
};

export const getUserGroupIdsForUser = (user: User): string[] => {
  return user.groupRoles.map((groupRole: UserGroupRole) => groupRole.groupId);
};

export const getEditorUserGroups = (user: User): UserGroupRole[] => {
  return user.groupRoles.filter((groupRole: UserGroupRole) => groupRole.roles.includes(GroupRole.Editor)) || [];
};

export const getApproverUserGroups = (user: User): UserGroupRole[] => {
  return user.groupRoles.filter((groupRole: UserGroupRole) => groupRole.roles.includes(GroupRole.Approver)) || [];
};

export const isEditor = (user: User): boolean => {
  return getPermissionsForUser(user).groups?.some((g) => g.roles.includes(GroupRole.Editor)) || false;
};

export const isEditorForDataset = (user: User, dataset: Dataset): boolean => {
  if (!user.groupRoles || !dataset.userGroupId) return false;
  return getEditorUserGroups(user).some((g: UserGroupRole) => g.group.id === dataset.userGroupId);
};

export const isApprover = (user: User): boolean => {
  return getPermissionsForUser(user).groups?.some((g) => g.roles.includes(GroupRole.Approver)) || false;
};

export const isApproverForDataset = (user: User, dataset: Dataset): boolean => {
  if (!user.groupRoles || !dataset.userGroupId) return false;
  return getApproverUserGroups(user).some((g: UserGroupRole) => g.group.id === dataset.userGroupId);
};
