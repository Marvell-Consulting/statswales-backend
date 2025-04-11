import passport from 'passport';
import { Issuer, Strategy as OpenIdStrategy, TokenSet, UserinfoResponse } from 'openid-client';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';
import { Strategy as JWTStrategy, ExtractJwt } from 'passport-jwt';
import { DataSource, Repository } from 'typeorm';
import { isEqual, pick } from 'lodash';

import { logger } from '../utils/logger';
import { User } from '../entities/user/user';
import { appConfig } from '../config';
import { AuthProvider } from '../enums/auth-providers';
import { asyncLocalStorage } from '../services/async-local-storage';
import { Locale } from '../enums/locale';
import { UserDTO } from '../dtos/user/user-dto';

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

  if (config.auth.providers.includes(AuthProvider.Google)) {
    try {
      await initGoogle(userRepository, config.auth.google);
      logger.debug('Google auth initialized');
    } catch (error) {
      logger.error(error, 'could not initialize Google auth');
    }
  }

  logger.info('Authentication providers initialized');
};

const initJwt = async (userRepository: Repository<User>, jwtConfig: Record<string, any>): Promise<void> => {
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

          // convert user dto to a plain object so we can compare with jwt payload
          const refreshedUser = JSON.parse(JSON.stringify(UserDTO.fromUser(user, Locale.English)));

          // compare the props that control permissions and force reauthentication if they are different
          const permissionsProps = ['id', 'global_roles', 'groups', 'status'];
          const jwtPerms = pick(jwtUser, permissionsProps);
          const activePerms = pick(refreshedUser, permissionsProps);

          if (!isEqual(jwtPerms, activePerms)) {
            logger.warn('User permissions have changed, user should re-authenticate');
            done(null, undefined, { message: 'User permissions have changed, please re-authenticate' });
            return;
          }

          // store the user context for code that does not have access to the request object
          asyncLocalStorage.getStore()?.set('user', user);

          logger.info('user successfully authenticated');
          done(null, user);
        } catch (err: any) {
          logger.error(err);
          done(null, undefined, { message: 'Unknown error' });
        }
      }
    )
  );
};

const initEntraId = async (userRepository: Repository<User>, entraIdConfig: Record<string, any>): Promise<void> => {
  if (!entraIdConfig.url || !entraIdConfig.clientId || !entraIdConfig.clientSecret) {
    throw new Error('EntraId configuration is missing');
  }

  const issuer = await Issuer.discover(`${entraIdConfig.url}/.well-known/openid-configuration`);

  passport.use(
    AuthProvider.EntraId,
    new OpenIdStrategy(
      {
        client: new issuer.Client({
          client_id: entraIdConfig.clientId,
          client_secret: entraIdConfig.clientSecret,
          redirect_uris: [`${config.backend.url}/auth/entraid/callback`]
        }),
        params: {
          scope: 'openid profile email',
          prompt: 'select_account'
        }
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      async (tokenset: TokenSet, userInfo: UserinfoResponse, done: any): Promise<void> => {
        logger.debug('auth callback from entraid received');

        if (!userInfo?.sub || !userInfo?.email) {
          logger.error('entraid auth failed: account is missing user id or email address and we need both');
          done(null, undefined, { message: 'entraid account does not have a user id or email, cannot login' });
          return;
        }

        try {
          // EntraID seems to only provide full name, splitting it this way might not give us the correct
          // given/family name order depending on the user's culture
          const [givenName, familyName] = userInfo.name ? userInfo.name.split(' ') : [undefined, undefined];

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
                email: userInfo.email,
                givenName,
                familyName,
                lastLoginAt: new Date()
              })
              .save();

            done(null, existingUserById);
            return;
          }

          logger.debug('no previous login found, falling back to email...');
          const existingUserByEmail = await userRepository.findOne({
            where: { email: userInfo.email },
            relations: { groupRoles: { group: { metadata: true } } }
          });

          if (existingUserByEmail) {
            logger.debug('user found by email, associating user record with entraid account');

            await userRepository
              .merge(existingUserByEmail, {
                provider: AuthProvider.EntraId,
                providerUserId: userInfo.sub,
                givenName,
                familyName,
                lastLoginAt: new Date()
              })
              .save();
          }

          logger.error('No matching user found, cannot log in');
          done(null, undefined, { message: 'User not recognised' });
          return;
        } catch (error) {
          logger.error(error);
          done(null, undefined, { message: 'Unknown error' });
        }
      }
    )
  );
};

const initGoogle = async (userRepository: Repository<User>, googleConfig: Record<string, any>): Promise<void> => {
  if (!googleConfig.clientId || !googleConfig.clientSecret) {
    throw new Error('Google configuration is missing');
  }

  passport.use(
    AuthProvider.Google,
    new GoogleStrategy(
      {
        clientID: googleConfig.clientId,
        clientSecret: googleConfig.clientSecret,
        callbackURL: `${config.backend.url}/auth/google/callback`,
        scope: ['openid', 'profile', 'email']
      },
      async (accessToken, refreshToken, profile, done): Promise<void> => {
        logger.debug('auth callback from google received');

        if (!profile?.id || !profile?._json?.email) {
          logger.error('google auth failed: account is missing user id or email address and we need both');
          done(null, undefined, { message: 'google account does not have a user id or email, cannot login' });
          return;
        }

        try {
          logger.debug('checking if user has previously logged in...');
          const existingUserById = await userRepository.findOne({
            where: {
              provider: AuthProvider.Google,
              providerUserId: profile?.id
            },
            relations: { groupRoles: { group: { metadata: true } } }
          });

          if (existingUserById) {
            logger.debug('user found by provider id, updating user record with latest details from google');

            await userRepository
              .merge(existingUserById, {
                email: profile._json.email,
                givenName: profile.name?.givenName,
                familyName: profile.name?.familyName,
                lastLoginAt: new Date()
              })
              .save();

            done(null, existingUserById);
            return;
          }

          logger.debug('no previous login found, falling back to email...');
          const existingUserByEmail = await userRepository.findOne({
            where: { email: profile._json.email },
            relations: { groupRoles: { group: { metadata: true } } }
          });

          if (existingUserByEmail) {
            logger.debug('user found by email, associating user record with google account');

            await userRepository
              .merge(existingUserByEmail, {
                provider: AuthProvider.Google,
                providerUserId: profile.id,
                givenName: profile.name?.givenName,
                familyName: profile.name?.familyName,
                lastLoginAt: new Date()
              })
              .save();
          }

          logger.error('No matching user found, cannot log in');
          done(null, undefined, { message: 'User not recognised' });
          return;
        } catch (error) {
          logger.error(error);
          done(null, undefined, { message: 'Unknown error' });
        }
      }
    )
  );
};
