import { createPrivateKey } from 'node:crypto';

import passport from 'passport';
import { Issuer, Strategy as OpenIdStrategy, TokenSet, UserinfoResponse } from 'openid-client';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';
import { Strategy as JWTStrategy, ExtractJwt } from 'passport-jwt';
import { Repository } from 'typeorm';

import { logger } from '../utils/logger';
import { User } from '../entities/user';
import { appConfig } from '../config';

const config = appConfig();

const readPrivateKey = (privateKey: string) => {
    return createPrivateKey({ key: Buffer.from(privateKey, 'base64'), type: 'pkcs8', format: 'der' });
};

export const initPassport = async (userRepository: Repository<User>): Promise<void> => {
    passport.use(
        'jwt',
        new JWTStrategy(
            {
                jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
                secretOrKey: config.auth.jwt.secret
            },
            async (jwtPayload, done) => {
                logger.info('request authenticated with JWT');
                const user = await userRepository.findOneBy({ id: jwtPayload?.user?.id });
                return done(null, user);
            }
        )
    );

    if (config.auth.providers.includes('onelogin')) {
        const oneLoginIssuer = await Issuer.discover(`${config.auth.oneLogin.url}/.well-known/openid-configuration`);
        const privateKey = config.auth.oneLogin.privateKey.replace(/\\n/g, '\n');

        passport.use(
            'onelogin',
            new OpenIdStrategy(
                {
                    client: new oneLoginIssuer.Client(
                        {
                            client_id: config.auth.oneLogin.clientId,
                            client_secret: config.auth.oneLogin.clientSecret,
                            redirect_uris: [`${config.backend.url}/auth/onelogin/callback`],
                            token_endpoint_auth_method: 'private_key_jwt',
                            token_endpoint_auth_signing_alg: 'PS256',
                            id_token_signed_response_alg: 'ES256'
                        },
                        {
                            keys: [readPrivateKey(privateKey).export({ format: 'jwk' })]
                        }
                    ),
                    params: {
                        response_type: 'code',
                        scope: 'openid email'
                        // TODO: we need to update our OneLogin config to be able to request the below claims
                        // vtr: ['P2.Cl.Cm'],
                        // claims: {
                        //     userinfo: {
                        //         'https://vocab.account.gov.uk/v1/coreIdentityJWT': null
                        //     }
                        // }
                    }
                },
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                async (tokenset: TokenSet, userInfo: UserinfoResponse, cb: any): Promise<void> => {
                    logger.debug('auth callback from onelogin received');

                    if (userInfo.email === undefined) {
                        logger.error('onelogin auth failed: account has no email address');
                        cb(null, undefined, {
                            message: 'onelogin account does not have an email, use another provider'
                        });
                        return;
                    }

                    try {
                        logger.debug('checking if user has previously logged in...');
                        const existingUser = await userRepository.findOneBy({ email: userInfo.email });

                        if (existingUser && existingUser.provider !== 'onelogin') {
                            logger.error('onelogin auth failed: email was registered via another provider');
                            cb(null, undefined, { message: 'User is already registered via another provider' });
                            return;
                        }

                        if (!existingUser) {
                            logger.debug('no previous login found, creating new user');

                            const user = await userRepository.save({
                                provider: 'onelogin',
                                providerUserId: userInfo.sub,
                                email: userInfo.email,
                                emailVerified: userInfo.email_verified
                            });
                            cb(null, user);
                            return;
                        }
                        logger.debug('existing user found');
                        cb(null, existingUser);
                    } catch (error) {
                        logger.error(error);
                        cb(null, undefined, { message: 'Unknown error' });
                    }
                }
            )
        );
    }

    if (config.auth.providers.includes('google')) {
        passport.use(
            'google',
            new GoogleStrategy(
                {
                    clientID: config.auth.google.clientId,
                    clientSecret: config.auth.google.clientSecret,
                    callbackURL: `${config.backend.url}/auth/google/callback`,
                    scope: ['openid', 'profile', 'email']
                },
                async (accessToken, refreshToken, profile, cb): Promise<void> => {
                    logger.debug('auth callback from google received');

                    if (!profile?._json?.email) {
                        logger.error('google auth failed: account has no email address');
                        cb(null, undefined, {
                            message: 'google account does not have an email, use another provider'
                        });
                        return;
                    }

                    try {
                        logger.debug('checking if user has previously logged in...');
                        const existingUser = await userRepository.findOneBy({ email: profile._json.email });

                        if (existingUser && existingUser.provider !== 'google') {
                            logger.error('google auth failed: email was registered via another provider');
                            cb(null, undefined, { message: 'User is already registered via another provider' });
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
                            cb(null, user);
                            return;
                        }
                        logger.debug('existing user found');
                        cb(null, existingUser);
                    } catch (error) {
                        logger.error(error);
                        cb(null, undefined, { message: 'Unknown error' });
                    }
                }
            )
        );
    }

    logger.info('Passport initialized');
};
