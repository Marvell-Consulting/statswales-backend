import { URL } from 'node:url';

import { RequestHandler } from 'express';
import passport from 'passport';
import jwt from 'jsonwebtoken';
import { Repository } from 'typeorm';

import { appConfig } from '../config';
import { logger } from '../utils/logger';
import { User } from '../entities/user/user';
import { AuthProvider } from '../enums/auth-providers';
import { dataSource } from '../db/data-source';
import { UserDTO } from '../dtos/user/user-dto';
import { Locale } from '../enums/locale';

const config = appConfig();
const domain = new URL(config.auth.jwt.cookieDomain).hostname;
logger.debug(`JWT cookie domain is '${domain}'`);

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
    const payload = { user: UserDTO.fromUser(user, req.language as Locale) };
    const { secret, expiresIn, secure } = config.auth.jwt;
    const token = jwt.sign(payload, secret, { expiresIn });

    res.cookie('jwt', token, { secure, httpOnly: true, domain });
    res.redirect(returnURL);
  } catch (error) {
    logger.error(error);
    res.redirect(`${returnURL}?error=login`);
  }
};

export const loginGoogle: RequestHandler = (req, res, next) => {
  logger.debug('attempting to authenticate with Google...');

  const returnURL = `${config.frontend.url}/auth/callback`;

  passport.authenticate(AuthProvider.Google, (err: Error, user: User, info: Record<string, string>) => {
    if (err || !user) {
      const errorMessage = err?.message || info?.message || 'unknown error';
      logger.error(`google auth returned an error: ${errorMessage}`);
      res.redirect(`${returnURL}?error=provider`);
      return;
    }
    req.login(user, { session: false }, (error) => {
      if (error) {
        logger.error(`error logging in: ${error}`);
        res.redirect(`${returnURL}?error=login`);
        return;
      }

      logger.info('google auth successful, creating JWT and returning user to the frontend');

      const payload = { user: UserDTO.fromUser(user, req.language as Locale) };
      const { secret, expiresIn, secure } = config.auth.jwt;
      const token = jwt.sign(payload, secret, { expiresIn });

      res.cookie('jwt', token, { secure, httpOnly: true, domain });
      res.redirect(returnURL);
    });
  })(req, res, next);
};

export const loginEntraID: RequestHandler = (req, res, next) => {
  logger.debug('attempting to authenticate with EntraID...');

  const returnURL = `${config.frontend.url}/auth/callback`;

  passport.authenticate(AuthProvider.EntraId, (err: Error, user: User, info: Record<string, string>) => {
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

      const payload = { user: UserDTO.fromUser(user, req.language as Locale) };
      const { secret, expiresIn, secure } = config.auth.jwt;
      const token = jwt.sign(payload, secret, { expiresIn });

      res.cookie('jwt', token, { secure, httpOnly: true, domain });
      res.redirect(returnURL);
    });
  })(req, res, next);
};
