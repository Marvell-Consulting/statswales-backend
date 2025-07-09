import { DeepPartial } from 'typeorm';

import { group1, group2, group3 } from './group';
import { User } from '../../../entities/user/user';
import { GlobalRole } from '../../../enums/global-role';
import { GroupRole } from '../../../enums/group-role';

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
  groupRoles: [
    { id: '56e78eb3-f831-48ff-ab78-8183acab9060', groupId: group1.id, roles: [GroupRole.Editor] },
    { id: '5c051ad8-5689-4427-ab7d-49c782d1fb8a', groupId: group2.id, roles: [GroupRole.Editor] },
    { id: '7f2d9f32-b282-4b28-85fd-31209059d8ab', groupId: group3.id, roles: [GroupRole.Editor] }
  ]
};

export const approver1: DeepPartial<User> = {
  id: 'ce08727e-dd3f-48cc-921a-cae5c4dd4a18',
  provider: 'local',
  providerUserId: 'test_approver_1',
  givenName: 'Frank',
  familyName: 'Approver',
  email: 'frank.approver@example.com',
  groupRoles: [
    { id: '347b302e-ea1a-40b2-ad02-6bf5f4b85bc6', groupId: group1.id, roles: [GroupRole.Approver] },
    { id: '3f977628-cbbc-4bb9-bb42-a928f3f59773', groupId: group2.id, roles: [GroupRole.Approver] },
    { id: '8f6b4f72-9f2c-4e47-9e9a-327a9a07663d', groupId: group3.id, roles: [GroupRole.Approver] }
  ]
};

export const testUsers = [admin1, publisher1, approver1];
