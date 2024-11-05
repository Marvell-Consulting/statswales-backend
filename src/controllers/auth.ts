import { URL } from 'node:url';

import { RequestHandler } from 'express';
import passport from 'passport';
import jwt from 'jsonwebtoken';

import { appConfig } from '../config';
import { logger } from '../utils/logger';
import { User } from '../entities/user/user';
import { sanitiseUser } from '../utils/sanitise-user';

const config = appConfig();
const domain = new URL(config.auth.jwt.cookieDomain).hostname;
logger.debug(`JWT cookie domain is '${domain}'`);

export const loginGoogle: RequestHandler = (req, res, next) => {
    logger.debug('attempting to authenticate with google...');

    const returnURL = `${config.frontend.url}/auth/callback`;

    passport.authenticate('google', (err: Error, user: User, info: Record<string, string>) => {
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

            const payload = { user: sanitiseUser(user) };
            const { secret, expiresIn, secure } = config.auth.jwt;
            const token = jwt.sign(payload, secret, { expiresIn });

            res.cookie('jwt', token, { secure, httpOnly: true, domain });
            res.redirect(returnURL);
        });
    })(req, res, next);
};

export const loginOneLogin: RequestHandler = (req, res, next) => {
    logger.debug('attempting to authenticate with one-login...');

    const returnURL = `${config.frontend.url}/auth/callback`;

    passport.authenticate('onelogin', (err: Error, user: User, info: Record<string, string>) => {
        if (err || !user) {
            const errorMessage = err?.message || info?.message || 'unknown error';
            logger.error(`onelogin auth returned an error: ${errorMessage}`);
            res.redirect(`${returnURL}?error=provider`);
            return;
        }
        req.login(user, { session: false }, (error) => {
            if (error) {
                logger.error(`error logging in: ${error}`);
                res.redirect(`${returnURL}?error=login`);
                return;
            }

            logger.info('onelogin auth successful, creating JWT and returning user to the frontend');

            const payload = { user: sanitiseUser(user) };
            const { secret, expiresIn, secure } = config.auth.jwt;
            const token = jwt.sign(payload, secret, { expiresIn });

            res.cookie('jwt', token, { secure, httpOnly: true, domain });
            res.redirect(returnURL);
        });
    })(req, res, next);
};
