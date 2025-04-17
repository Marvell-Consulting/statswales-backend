import { DeepPartial } from 'typeorm';

import { User } from '../../src/entities/user/user';
import { GlobalRole } from '../../src/enums/global-role';
import { stage1Group } from '../../src/seeders/orgs-and-groups';
import { GroupRole } from '../../src/enums/group-role';

export const admin1: DeepPartial<User> = {
  id: '044d94c5-91ba-495e-a718-31c597a0a30b',
  provider: 'local',
  providerUserId: 'test_admin_1',
  givenName: 'Tom',
  familyName: 'Admin',
  email: 'tom.admin@example.com',
  globalRoles: [GlobalRole.ServiceAdmin]
};

export const publisher1: DeepPartial<User> = {
  id: 'f3dc1ae6-273e-4ac9-a498-ba2813c51c24',
  provider: 'local',
  providerUserId: 'test_publisher_1',
  givenName: 'Joe',
  familyName: 'Publisher',
  email: 'joe.publisher@example.com',
  groupRoles: [{ groupId: stage1Group.id, roles: [GroupRole.Editor] }]
};

export const approver1: DeepPartial<User> = {
  id: 'ce08727e-dd3f-48cc-921a-cae5c4dd4a18',
  provider: 'local',
  providerUserId: 'test_approver_1',
  givenName: 'Frank',
  familyName: 'Approver',
  email: 'frank.approver@example.com',
  groupRoles: [{ groupId: stage1Group.id, roles: [GroupRole.Approver] }]
};

export const testUsers = [admin1, publisher1, approver1];
