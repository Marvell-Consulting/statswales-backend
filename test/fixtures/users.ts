import { DeepPartial } from 'typeorm';

import { User } from '../../src/entities/user/user';

export const admin1: DeepPartial<User> = {
  id: '044d94c5-91ba-495e-a718-31c597a0a30b',
  provider: 'fixture',
  providerUserId: 'test_user_admin_1',
  givenName: 'Tom',
  familyName: 'Admin',
  email: 'tom.admin@example.com',
  emailVerified: true
};

export const publisher1: DeepPartial<User> = {
  id: 'f3dc1ae6-273e-4ac9-a498-ba2813c51c24',
  provider: 'fixture',
  providerUserId: 'test_user_publisher_1',
  givenName: 'Joe',
  familyName: 'Publisher',
  email: 'joe.publisher@example.com',
  emailVerified: true
};

export const approver1: DeepPartial<User> = {
  id: 'ce08727e-dd3f-48cc-921a-cae5c4dd4a18',
  provider: 'fixture',
  providerUserId: 'test_user_approver_1',
  givenName: 'Frank',
  familyName: 'Approver',
  email: 'frank.approver@example.com',
  emailVerified: true
};

export const testUsers = [admin1, publisher1, approver1];
