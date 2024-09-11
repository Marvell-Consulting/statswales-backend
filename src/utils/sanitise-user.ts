import { pick } from 'lodash';

import { User } from '../entities/user';

// strip anything from the user object that we do not want to expose to the client
export const sanitiseUser = (user: User): Partial<User> => {
    return pick(user, ['id', 'email', 'givenName', 'familyName']);
};
