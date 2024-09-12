import { randomUUID } from 'node:crypto';

import { capitalize } from 'lodash';

import { User } from '../../src/entities/user';

export const getTestUser = (givenName = 'test', familyName = 'user'): User => {
    const user = new User();
    user.email = `${givenName}.${familyName}@example.com`;
    user.emailVerified = true;
    user.provider = 'test';
    user.providerUserId = randomUUID().toLowerCase();
    user.givenName = capitalize(givenName);
    user.familyName = capitalize(familyName);
    return user;
};
