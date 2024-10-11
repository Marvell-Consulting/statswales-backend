import jwt from 'jsonwebtoken';

import { User } from '../../src/entities/dataset/user';
import { sanitiseUser } from '../../src/utils/sanitise-user';
import { appConfig } from '../../src/config';

const config = appConfig();

export const getAuthHeader = (user: User) => {
    const payload = { user: sanitiseUser(user) };
    const token = jwt.sign(payload, config.auth.jwt.secret);
    return { Authorization: `Bearer ${token}` };
};
