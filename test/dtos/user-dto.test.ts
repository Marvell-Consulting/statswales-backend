jest.mock('../../src/dtos/user/user-group-dto', () => ({
  UserGroupDTO: { fromUserGroup: jest.fn().mockReturnValue({ id: 'ug-stub' }) }
}));

import { User } from '../../src/entities/user/user';
import { GroupRole } from '../../src/enums/group-role';
import { Locale } from '../../src/enums/locale';
import { UserDTO } from '../../src/dtos/user/user-dto';
import { UserGroupDTO } from '../../src/dtos/user/user-group-dto';

describe('UserDTO', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const makeUser = (overrides = {}): User => {
    return {
      id: 'user-1',
      provider: 'entra-id',
      providerUserId: 'ext-123',
      email: 'test@example.com',
      name: 'Test User',
      status: 'active',
      createdAt: new Date('2025-01-01T00:00:00Z'),
      updatedAt: new Date('2025-02-01T00:00:00Z'),
      lastLoginAt: new Date('2025-03-01T00:00:00Z'),
      globalRoles: ['admin'],
      groupRoles: [
        {
          groupId: 'g1',
          group: { id: 'g1' },
          roles: [GroupRole.Editor]
        }
      ],
      ...overrides
    } as unknown as User;
  };

  describe('fromUser', () => {
    it('should map scalar fields correctly', () => {
      const dto = UserDTO.fromUser(makeUser(), Locale.English);

      expect(dto.id).toBe('user-1');
      expect(dto.provider).toBe('entra-id');
      expect(dto.provider_user_id).toBe('ext-123');
      expect(dto.email).toBe('test@example.com');
      expect(dto.name).toBe('Test User');
      expect(dto.status).toBe('active');
    });

    it('should convert dates to ISO strings', () => {
      const dto = UserDTO.fromUser(makeUser(), Locale.English);

      expect(dto.created_at).toBe('2025-01-01T00:00:00.000Z');
      expect(dto.updated_at).toBe('2025-02-01T00:00:00.000Z');
      expect(dto.last_login_at).toBe('2025-03-01T00:00:00.000Z');
    });

    it('should handle null dates', () => {
      const dto = UserDTO.fromUser(makeUser({ createdAt: null, updatedAt: null, lastLoginAt: null }), Locale.English);

      expect(dto.created_at).toBeUndefined();
      expect(dto.updated_at).toBeUndefined();
      expect(dto.last_login_at).toBeUndefined();
    });

    it('should default globalRoles to empty array when null', () => {
      const dto = UserDTO.fromUser(makeUser({ globalRoles: null }), Locale.English);

      expect(dto.global_roles).toEqual([]);
    });

    it('should map globalRoles when present', () => {
      const dto = UserDTO.fromUser(makeUser({ globalRoles: ['admin', 'superuser'] }), Locale.English);

      expect(dto.global_roles).toEqual(['admin', 'superuser']);
    });

    it('should delegate group mapping to UserGroupDTO with lang', () => {
      const dto = UserDTO.fromUser(makeUser(), Locale.Welsh);

      expect(UserGroupDTO.fromUserGroup).toHaveBeenCalledWith({ id: 'g1' }, Locale.Welsh);
      expect(dto.groups).toHaveLength(1);
      expect(dto.groups[0].group).toEqual({ id: 'ug-stub' });
      expect(dto.groups[0].roles).toEqual([GroupRole.Editor]);
    });

    it('should handle empty groupRoles', () => {
      const dto = UserDTO.fromUser(makeUser({ groupRoles: [] }), Locale.English);

      expect(dto.groups).toEqual([]);
    });

    it('should handle null groupRoles', () => {
      const dto = UserDTO.fromUser(makeUser({ groupRoles: null }), Locale.English);

      expect(dto.groups).toEqual([]);
    });
  });

  describe('fromUserForJWT', () => {
    it('should map only minimal fields', () => {
      const dto = UserDTO.fromUserForJWT(makeUser());

      expect(dto.id).toBe('user-1');
      expect(dto.email).toBe('test@example.com');
      expect(dto.name).toBe('Test User');
      expect(dto.status).toBe('active');
      expect(dto.provider).toBeUndefined();
      expect(dto.provider_user_id).toBeUndefined();
    });

    it('should use groupId directly instead of calling UserGroupDTO', () => {
      const dto = UserDTO.fromUserForJWT(makeUser());

      expect(UserGroupDTO.fromUserGroup).not.toHaveBeenCalled();
      expect(dto.groups[0].group).toEqual({ id: 'g1' });
      expect(dto.groups[0].roles).toEqual([GroupRole.Editor]);
    });
  });
});
