import { URL } from 'node:url';

import { RequestHandler } from 'express';
import passport, { AuthenticateOptions } from 'passport';
import jwt from 'jsonwebtoken';
import { Repository } from 'typeorm';

import { appConfig } from '../config';
import { logger } from '../utils/logger';
import { User } from '../entities/user/user';
import { AuthProvider } from '../enums/auth-providers';
import { dataSource } from '../db/data-source';
import { UserDTO } from '../dtos/user/user-dto';

const config = appConfig();
const domain = new URL(config.auth.jwt.cookieDomain).hostname;
logger.debug(`JWT cookie domain is '${domain}'`);

const checkTokenFitsInCookie = (token: string): void => {
  const maxCookieSize = 4096; // Maximum size of a cookie in bytes
  const tokenSize = Buffer.byteLength(token, 'utf8');

  if (tokenSize > maxCookieSize) {
    // our JWTs include a list of the user groups and roles - if the user has many groups the JWT can become too large
    // to fit in a cookie. This is a limitation of cookies, not JWTs themselves.
    // TODO: Consider using a different mechanism to fetch and store user roles/groups on the frontend
    throw new Error(`JWT token size (${tokenSize} bytes) exceeds the maximum cookie size (${maxCookieSize} bytes).`);
  } else {
    logger.debug(`JWT token is ${tokenSize} bytes (max: ${maxCookieSize})`);
  }
};

// should only ever be used in testing environments
export const loginLocal: RequestHandler = async (req, res) => {
  logger.debug('auth request from local form received');

  const returnURL = `${config.frontend.url}/auth/callback`;
  const username = ((req.query.username as string) || '').trim();

  if (!username) {
    logger.error('local auth failed: username must be provided');
    res.redirect(`${returnURL}?error=login`);
    return;
  }

  try {
    logger.debug('checking if user exists...');
    const userRepository: Repository<User> = dataSource.getRepository('User');
    const user = await userRepository.findOneOrFail({
      where: { providerUserId: username, provider: 'local' },
      relations: { groupRoles: { group: { metadata: true } } }
    });
    logger.debug('existing user found');

    logger.info('local auth successful, creating JWT and returning user to the frontend');
    const payload = { user: UserDTO.fromUserForJWT(user) };
    const { secret, expiresIn, secure } = config.auth.jwt;
    const token = jwt.sign(payload, secret, { expiresIn });
    checkTokenFitsInCookie(token);

    res.cookie('jwt', token, { secure, httpOnly: true, domain });
    res.redirect(returnURL);
  } catch (error) {
    logger.error(error);
    res.redirect(`${returnURL}?error=login`);
  }
};

export const loginEntraID: RequestHandler = (req, res, next) => {
  logger.debug('attempting to authenticate with EntraID...');

  const returnURL = `${config.frontend.url}/auth/callback`;
  const opts: AuthenticateOptions = { prompt: 'select_account' };

  passport.authenticate(AuthProvider.EntraId, opts, (err: Error, user: User, info: Record<string, string>) => {
    if (err || !user) {
      const errorMessage = err?.message || info?.message || 'unknown error';
      logger.error(`entraid auth returned an error: ${errorMessage}`);
      res.redirect(`${returnURL}?error=provider`);
      return;
    }
    req.login(user, { session: false }, (error) => {
      if (error) {
        logger.error(`error logging in: ${error}`);
        res.redirect(`${returnURL}?error=login`);
        return;
      }

      logger.info('entraid auth successful, creating JWT and returning user to the frontend');
      const payload = { user: UserDTO.fromUserForJWT(user) };
      const { secret, expiresIn, secure } = config.auth.jwt;
      const token = jwt.sign(payload, secret, { expiresIn });
      checkTokenFitsInCookie(token);

      res.cookie('jwt', token, { secure, httpOnly: true, domain });
      res.redirect(returnURL);
    });
  })(req, res, next);
};
