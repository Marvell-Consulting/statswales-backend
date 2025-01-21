import passport from 'passport';
import { Issuer, Strategy as OpenIdStrategy, TokenSet, UserinfoResponse } from 'openid-client';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';
import { Strategy as JWTStrategy, ExtractJwt } from 'passport-jwt';
import { DataSource, Repository } from 'typeorm';

import { logger } from '../utils/logger';
import { User } from '../entities/user/user';
import { appConfig } from '../config';
import { AuthProvider } from '../enums/auth-providers';
import { asyncLocalStorage } from '../services/async-local-storage';

const config = appConfig();

export const initPassport = async (dataSource: DataSource): Promise<void> => {
    const userRepository: Repository<User> = dataSource.getRepository('User');

    passport.use(
        AuthProvider.Jwt,
        new JWTStrategy(
            {
                jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
                secretOrKey: config.auth.jwt.secret
            },
            async (jwtPayload, done): Promise<void> => {
                logger.debug('authenticating request with JWT...');

                try {
                    const user = await userRepository.findOneBy({ id: jwtPayload?.user?.id });

                    if (!user) {
                        logger.error('jwt auth failed: user account could not be found');
                        done(null, undefined, { message: 'User not recognised' });
                        return;
                    }

                    // store the user context for code that does not have access to the request object
                    asyncLocalStorage.getStore()?.set('user', user);

                    done(null, user);
                } catch (err: any) {
                    logger.error(err);
                    done(null, undefined, { message: 'Unknown error' });
                }
            }
        )
    );

    if (config.auth.providers.includes(AuthProvider.EntraId)) {
        const issuer = await Issuer.discover(`${config.auth.entraid.url}/.well-known/openid-configuration`);

        passport.use(
            AuthProvider.EntraId,
            new OpenIdStrategy(
                {
                    client: new issuer.Client({
                        client_id: config.auth.entraid.clientId,
                        client_secret: config.auth.entraid.clientSecret,
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

                    if (userInfo.email === undefined) {
                        logger.error('entraid auth failed: account has no email address');
                        done(null, undefined, {
                            message: 'entraid account does not have an email, use another provider'
                        });
                        return;
                    }

                    try {
                        logger.debug('checking if user has previously logged in...');
                        const existingUser = await userRepository.findOneBy({ email: userInfo.email });

                        if (existingUser && existingUser.provider !== 'entraid') {
                            logger.warn(
                                `entraid: email was previously used via another provider (${existingUser.provider})`
                            );

                            // TODO: find a better way to merge providers rather than overwriting
                            existingUser.provider = 'entraid';
                            existingUser.providerUserId = userInfo.sub;
                            await existingUser.save();
                            done(null, existingUser);
                            return;
                        }

                        if (!existingUser) {
                            logger.debug('no previous login found, creating new user');

                            // TODO: EntraID only provides full name, we might want to avoid splitting it
                            const [givenName, familyName] = userInfo.name
                                ? userInfo.name.split(' ')
                                : [undefined, undefined];

                            const user = await userRepository.save({
                                provider: 'entraid',
                                providerUserId: userInfo.sub,
                                email: userInfo.email,
                                emailVerified: undefined,
                                givenName,
                                familyName
                            });
                            done(null, user);
                            return;
                        }
                        logger.debug('existing user found');
                        done(null, existingUser);
                    } catch (error) {
                        logger.error(error);
                        done(null, undefined, { message: 'Unknown error' });
                    }
                }
            )
        );
    }

    if (config.auth.providers.includes(AuthProvider.Google)) {
        passport.use(
            AuthProvider.Google,
            new GoogleStrategy(
                {
                    clientID: config.auth.google.clientId,
                    clientSecret: config.auth.google.clientSecret,
                    callbackURL: `${config.backend.url}/auth/google/callback`,
                    scope: ['openid', 'profile', 'email']
                },
                async (accessToken, refreshToken, profile, done): Promise<void> => {
                    logger.debug('auth callback from google received');

                    if (!profile?._json?.email) {
                        logger.error('google auth failed: account has no email address');
                        done(null, undefined, {
                            message: 'google account does not have an email, use another provider'
                        });
                        return;
                    }

                    try {
                        logger.debug('checking if user has previously logged in...');
                        const existingUser = await userRepository.findOneBy({ email: profile._json.email });

                        if (existingUser && existingUser.provider !== 'google') {
                            logger.warn(
                                `google: email was previously used via another provider (${existingUser.provider})`
                            );

                            // TODO: find a better way to merge providers rather than overwriting
                            existingUser.provider = 'google';
                            existingUser.providerUserId = profile.id;
                            await existingUser.save();
                            done(null, existingUser);
                            return;
                        }

                        if (!existingUser) {
                            logger.debug('no previous login found, creating new user');

                            const user = await userRepository.save({
                                provider: 'google',
                                providerUserId: profile.id,
                                email: profile._json.email,
                                emailVerified: profile._json.email_verified,
                                givenName: profile.name?.givenName,
                                familyName: profile.name?.familyName
                            });
                            done(null, user);
                            return;
                        }
                        logger.debug('existing user found');
                        done(null, existingUser);
                    } catch (error) {
                        logger.error(error);
                        done(null, undefined, { message: 'Unknown error' });
                    }
                }
            )
        );
    }

    logger.info('Passport initialized');
};
