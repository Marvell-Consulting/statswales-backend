import { DeepPartial } from 'typeorm';

import { User } from '../../src/entities/user/user';
import { UserGroup } from '../../src/entities/user/user-group';
import { Locale } from '../../src/enums/locale';
import { uuidV4 } from '../../src/utils/uuid';

export const getTestUser = (name = 'test user'): User => {
  const user = new User();
  user.email = `${name.replace(' ', '.')}@example.com`;
  user.provider = 'local';
  user.providerUserId = uuidV4();
  user.name = name;
  return user;
};

export const getTestUserGroup = (name = 'test'): DeepPartial<UserGroup> => {
  return {
    id: uuidV4(),
    metadata: [
      { name: `${name} EN`, language: Locale.EnglishGb },
      { name: `${name} CY`, language: Locale.WelshGb }
    ]
  };
};
