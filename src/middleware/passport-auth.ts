import passport, { AuthenticateCallback } from 'passport';
import { Strategy as JWTStrategy, ExtractJwt } from 'passport-jwt';
import * as openIdClient from 'openid-client';
import { Strategy as OpenIdStrategy, type StrategyOptions, type VerifyFunction } from 'openid-client/passport';
import { DataSource, Repository } from 'typeorm';
import { isEqual } from 'lodash';

import { logger } from '../utils/logger';
import { User } from '../entities/user/user';
import { appConfig } from '../config';
import { AuthProvider } from '../enums/auth-providers';
import { asyncLocalStorage } from '../services/async-local-storage';
import { UserDTO } from '../dtos/user/user-dto';
import { getPermissionsForUserDTO } from '../utils/get-permissions-for-user';
import { EntraIdConfig, JWTConfig } from '../config/app-config.interface';

type Tokens = openIdClient.TokenEndpointResponse & openIdClient.TokenEndpointResponseHelpers;
type OpenIdConfig = openIdClient.Configuration;

const config = appConfig();

export const initPassport = async (dataSource: DataSource): Promise<void> => {
  logger.info('Configuring authentication providers...');
  const userRepository: Repository<User> = dataSource.getRepository('User');

  try {
    await initJwt(userRepository, config.auth.jwt);
    logger.debug('JWT auth initialized');
  } catch (error) {
    logger.error(error, 'could not initialize JWT auth');
  }

  if (config.auth.providers.includes(AuthProvider.EntraId)) {
    try {
      await initEntraId(userRepository, config.auth.entraid);
      logger.debug('EntraID auth initialized');
    } catch (error) {
      logger.error(error, 'could not initialize EntraId auth');
    }
  }

  logger.info('Authentication providers initialized');
};

const initJwt = async (userRepository: Repository<User>, jwtConfig: JWTConfig): Promise<void> => {
  if (!jwtConfig.secret) {
    throw new Error('JWT configuration is missing');
  }

  passport.use(
    AuthProvider.Jwt,
    new JWTStrategy(
      {
        jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
        secretOrKey: jwtConfig.secret
      },
      async (jwtPayload, done): Promise<void> => {
        logger.debug('authenticating request with JWT...');
        const jwtUser = jwtPayload.user;

        try {
          const user = await userRepository.findOne({
            where: { id: jwtUser?.id },
            relations: { groupRoles: { group: { metadata: true } } }
          });

          if (!user) {
            logger.error('jwt auth failed: user account could not be found');
            done(null, undefined, { message: 'User not recognised' });
            return;
          }

          // compare the props that control permissions and force reauthentication if they are different
          // need to jsonify user object to convert to plain object for comparison
          const refreshedUser = JSON.parse(JSON.stringify(UserDTO.fromUserForJWT(user)));
          const activePerms = getPermissionsForUserDTO(refreshedUser);
          const jwtPerms = getPermissionsForUserDTO(jwtUser);

          if (!isEqual(jwtPerms, activePerms)) {
            logger.warn({ jwtPerms, activePerms }, 'User permissions have changed, user should re-authenticate');
            done(null, undefined, { message: 'User permissions have changed, please re-authenticate' });
            return;
          }

          // store the user context for code that does not have access to the request object
          asyncLocalStorage.getStore()?.set('user', user);

          logger.info('user successfully authenticated');
          done(null, user);
        } catch (err) {
          logger.error(err);
          done(null, undefined, { message: 'Unknown error' });
        }
      }
    )
  );
};

const initEntraId = async (userRepository: Repository<User>, entraIdConfig: EntraIdConfig): Promise<void> => {
  if (!entraIdConfig.url || !entraIdConfig.clientId || !entraIdConfig.clientSecret) {
    throw new Error('entraid configuration is missing');
  }

  const openidConfig: OpenIdConfig = await openIdClient.discovery(
    new URL(entraIdConfig.url),
    entraIdConfig.clientId,
    entraIdConfig.clientSecret
  );

  const strategyOptions: StrategyOptions = {
    config: openidConfig,
    scope: 'openid profile email',
    callbackURL: `${config.backend.url}/auth/entraid/callback`
  };

  const verify: VerifyFunction = async (tokens: Tokens, done: AuthenticateCallback) => {
    logger.debug('auth callback from entraid received');
    const { sub } = tokens.claims()!;

    logger.debug('fetching user info from entraid...');
    const userInfo = await openIdClient.fetchUserInfo(openidConfig, tokens.access_token, sub);

    if (!userInfo?.sub || !userInfo?.email) {
      logger.error('entraid auth failed: account is missing user id or email address and we need both');
      done(null, undefined, { message: 'entraid account does not have a user id or email, cannot login' });
      return;
    }

    try {
      logger.debug('checking if user has previously logged in...');

      const existingUserById = await userRepository.findOne({
        where: {
          provider: AuthProvider.EntraId,
          providerUserId: userInfo.sub
        },
        relations: { groupRoles: { group: { metadata: true } } }
      });

      if (existingUserById) {
        logger.debug('user found by provider id, updating user record with latest details from entraid');

        await userRepository
          .merge(existingUserById, {
            email: userInfo.email.toLowerCase(),
            name: userInfo.name,
            lastLoginAt: new Date()
          })
          .save();

        done(null, existingUserById);
        return;
      }

      logger.debug('no previous login found, falling back to email...');
      const existingUserByEmail = await userRepository.findOne({
        where: { email: userInfo.email.toLowerCase() },
        relations: { groupRoles: { group: { metadata: true } } }
      });

      if (existingUserByEmail) {
        logger.debug('user found by email, associating user record with entraid account');

        await userRepository
          .merge(existingUserByEmail, {
            provider: AuthProvider.EntraId,
            providerUserId: userInfo.sub,
            name: userInfo.name,
            lastLoginAt: new Date()
          })
          .save();

        done(null, existingUserByEmail);
        return;
      }

      logger.error('No matching user found, cannot log in');
      done(null, undefined, { message: 'User not recognised' });
      return;
    } catch (error) {
      logger.error(error);
      done(null, undefined, { message: 'Unknown error' });
    }
  };

  passport.use(AuthProvider.EntraId, new OpenIdStrategy(strategyOptions, verify));
};
