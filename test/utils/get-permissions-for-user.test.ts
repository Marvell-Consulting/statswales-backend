import { Dataset } from '../../src/entities/dataset/dataset';
import { User } from '../../src/entities/user/user';
import { GroupRole } from '../../src/enums/group-role';
import {
  getApproverUserGroups,
  getEditorUserGroups,
  getPermissionsForUser,
  getPermissionsForUserDTO,
  getUserGroupIdsForUser,
  isApprover,
  isApproverForDataset,
  isEditor,
  isEditorForDataset
} from '../../src/utils/get-permissions-for-user';
import { UserDTO } from '../../src/dtos/user/user-dto';

describe('get-permissions-for-user', () => {
  const makeUser = (groupRoles: { groupId: string; roles: string[]; group?: { id: string } }[]): User => {
    return {
      id: 'user-1',
      globalRoles: ['admin'],
      status: 'active',
      groupRoles: groupRoles.map((gr) => ({
        groupId: gr.groupId,
        roles: gr.roles,
        group: gr.group || { id: gr.groupId }
      }))
    } as unknown as User;
  };

  describe('getPermissionsForUser', () => {
    it('should return id, globalRoles, status and mapped groups', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Editor] }]);
      const result = getPermissionsForUser(user);

      expect(result).toEqual({
        id: 'user-1',
        global_roles: ['admin'],
        status: 'active',
        groups: [{ id: 'g1', roles: [GroupRole.Editor] }]
      });
    });

    it('should handle multiple groups', () => {
      const user = makeUser([
        { groupId: 'g1', roles: [GroupRole.Editor] },
        { groupId: 'g2', roles: [GroupRole.Approver] }
      ]);
      const result = getPermissionsForUser(user);

      expect(result.groups).toHaveLength(2);
    });

    it('should handle empty groupRoles', () => {
      const user = makeUser([]);
      const result = getPermissionsForUser(user);

      expect(result.groups).toEqual([]);
    });
  });

  describe('getPermissionsForUserDTO', () => {
    it('should map UserDTO groups with group.id', () => {
      const dto = {
        id: 'user-1',
        global_roles: ['admin'],
        status: 'active',
        groups: [
          { group: { id: 'g1' }, roles: [GroupRole.Editor] },
          { group: { id: 'g2' }, roles: [GroupRole.Approver] }
        ]
      } as unknown as UserDTO;

      const result = getPermissionsForUserDTO(dto);

      expect(result).toEqual({
        id: 'user-1',
        global_roles: ['admin'],
        status: 'active',
        groups: [
          { id: 'g1', roles: [GroupRole.Editor] },
          { id: 'g2', roles: [GroupRole.Approver] }
        ]
      });
    });
  });

  describe('getUserGroupIdsForUser', () => {
    it('should return an array of groupIds', () => {
      const user = makeUser([
        { groupId: 'g1', roles: [GroupRole.Editor] },
        { groupId: 'g2', roles: [GroupRole.Approver] }
      ]);

      expect(getUserGroupIdsForUser(user)).toEqual(['g1', 'g2']);
    });

    it('should return empty array for user with no groups', () => {
      const user = makeUser([]);
      expect(getUserGroupIdsForUser(user)).toEqual([]);
    });
  });

  describe('getEditorUserGroups', () => {
    it('should return only groups with editor role', () => {
      const user = makeUser([
        { groupId: 'g1', roles: [GroupRole.Editor] },
        { groupId: 'g2', roles: [GroupRole.Approver] },
        { groupId: 'g3', roles: [GroupRole.Editor, GroupRole.Approver] }
      ]);

      const result = getEditorUserGroups(user);
      expect(result).toHaveLength(2);
      expect(result.map((r) => r.groupId)).toEqual(['g1', 'g3']);
    });
  });

  describe('getApproverUserGroups', () => {
    it('should return only groups with approver role', () => {
      const user = makeUser([
        { groupId: 'g1', roles: [GroupRole.Editor] },
        { groupId: 'g2', roles: [GroupRole.Approver] },
        { groupId: 'g3', roles: [GroupRole.Editor, GroupRole.Approver] }
      ]);

      const result = getApproverUserGroups(user);
      expect(result).toHaveLength(2);
      expect(result.map((r) => r.groupId)).toEqual(['g2', 'g3']);
    });
  });

  describe('isEditor', () => {
    it('should return true if user has editor role in any group', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Editor] }]);
      expect(isEditor(user)).toBe(true);
    });

    it('should return false if user has no editor role', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Approver] }]);
      expect(isEditor(user)).toBe(false);
    });

    it('should return false for user with no groups', () => {
      const user = makeUser([]);
      expect(isEditor(user)).toBe(false);
    });
  });

  describe('isEditorForDataset', () => {
    it('should return true if user is editor for the dataset user group', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Editor], group: { id: 'g1' } }]);
      const dataset = { userGroupId: 'g1' } as unknown as Dataset;

      expect(isEditorForDataset(user, dataset)).toBe(true);
    });

    it('should return false if user is editor for a different group', () => {
      const user = makeUser([{ groupId: 'g2', roles: [GroupRole.Editor], group: { id: 'g2' } }]);
      const dataset = { userGroupId: 'g1' } as unknown as Dataset;

      expect(isEditorForDataset(user, dataset)).toBe(false);
    });

    it('should return false if dataset has no userGroupId', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Editor], group: { id: 'g1' } }]);
      const dataset = { userGroupId: null } as unknown as Dataset;

      expect(isEditorForDataset(user, dataset)).toBe(false);
    });

    it('should return false if user has no groupRoles', () => {
      const user = { groupRoles: null } as unknown as User;
      const dataset = { userGroupId: 'g1' } as unknown as Dataset;

      expect(isEditorForDataset(user, dataset)).toBe(false);
    });
  });

  describe('isApprover', () => {
    it('should return true if user has approver role in any group', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Approver] }]);
      expect(isApprover(user)).toBe(true);
    });

    it('should return false if user has no approver role', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Editor] }]);
      expect(isApprover(user)).toBe(false);
    });
  });

  describe('isApproverForDataset', () => {
    it('should return true if user is approver for the dataset user group', () => {
      const user = makeUser([{ groupId: 'g1', roles: [GroupRole.Approver], group: { id: 'g1' } }]);
      const dataset = { userGroupId: 'g1' } as unknown as Dataset;

      expect(isApproverForDataset(user, dataset)).toBe(true);
    });

    it('should return false if user is approver for a different group', () => {
      const user = makeUser([{ groupId: 'g2', roles: [GroupRole.Approver], group: { id: 'g2' } }]);
      const dataset = { userGroupId: 'g1' } as unknown as Dataset;

      expect(isApproverForDataset(user, dataset)).toBe(false);
    });
  });
});
