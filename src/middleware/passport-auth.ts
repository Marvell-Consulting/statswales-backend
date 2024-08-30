import passport from 'passport';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';
import { Strategy as JWTStrategy, ExtractJwt } from 'passport-jwt';
import { Repository } from 'typeorm';

import { logger } from '../utils/logger';
import { User } from '../entity/user';

export const initPassport = (userRepository: Repository<User>): void => {
    passport.use(
        'jwt',
        new JWTStrategy(
            {
                jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
                secretOrKey: process.env.JWT_SECRET || ''
            },
            (jwtPayload, done) => {
                const user = jwtPayload.user;
                return done(null, user);
            }
        )
    );

    passport.use(
        'google',
        new GoogleStrategy(
            {
                clientID: process.env.GOOGLE_CLIENT_ID || '',
                clientSecret: process.env.GOOGLE_CLIENT_SECRET || '',
                callbackURL: `${process.env.BACKEND_URL}/auth/google/callback`,
                scope: ['openid', 'profile', 'email']
            },
            async (accessToken, refreshToken, profile, cb): Promise<void> => {
                logger.debug('auth callback from google received');

                if (!profile?._json?.email) {
                    logger.error('google auth failed: account has no email address');
                    cb(null, undefined, {
                        message: 'Google Account does not have an email, use another provider'
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
                            firstName: profile.name?.givenName,
                            lastName: profile.name?.familyName
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

    logger.info('Passport initialized');
};
