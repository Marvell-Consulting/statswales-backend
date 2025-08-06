import { randomUUID } from 'node:crypto';

import { DeepPartial } from 'typeorm';

import { User } from '../../src/entities/user/user';
import { UserGroup } from '../../src/entities/user/user-group';
import { Locale } from '../../src/enums/locale';

export const getTestUser = (name = 'test user'): User => {
  const user = new User();
  user.email = `${name.replace(' ', '.')}@example.com`;
  user.provider = 'local';
  user.providerUserId = randomUUID().toLowerCase();
  user.name = name;
  return user;
};

export const getTestUserGroup = (name = 'test'): DeepPartial<UserGroup> => {
  return {
    id: randomUUID().toLowerCase(),
    metadata: [
      { name: `${name} EN`, language: Locale.EnglishGb },
      { name: `${name} CY`, language: Locale.WelshGb }
    ]
  };
};
