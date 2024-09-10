import jwt from 'jsonwebtoken';

import { User } from '../../src/entities/user';
import { sanitiseUser } from '../../src/utils/sanitise-user';

export const getAuthHeader = (user: User) => {
    const payload = { user: sanitiseUser(user) };
    const token = jwt.sign(payload, process.env.JWT_SECRET!);
    return { Authorization: `Bearer ${token}` };
};
