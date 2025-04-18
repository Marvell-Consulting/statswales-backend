import jwt from 'jsonwebtoken';

import { UserDTO } from '../../src/dtos/user/user-dto';
import { User } from '../../src/entities/user/user';
import { appConfig } from '../../src/config';
import { Locale } from '../../src/enums/locale';

const config = appConfig();

export const getAuthHeader = (user: User) => {
  const payload = { user: UserDTO.fromUser(user, Locale.English) };
  const token = jwt.sign(payload, config.auth.jwt.secret);
  return { Authorization: `Bearer ${token}` };
};
