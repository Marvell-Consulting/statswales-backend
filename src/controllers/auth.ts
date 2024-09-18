import { URL } from 'node:url';

import { RequestHandler } from 'express';
import passport from 'passport';
import jwt from 'jsonwebtoken';

import { logger } from '../utils/logger';
import { User } from '../entities/user';
import { sanitiseUser } from '../utils/sanitise-user';

const returnURL = `${process.env.FRONTEND_URL}/auth/callback`;
const domain = new URL(process.env.BACKEND_URL || '').hostname.replace('statswales-develop-backend.', '');
logger.debug(`cookie domain: ${domain}`);

const DEFAULT_TOKEN_EXPIRY = '1d';

export const loginGoogle: RequestHandler = (req, res, next) => {
    logger.debug('attempting to authenticate with google...');

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
            const expiresIn = process.env.JWT_EXPIRES_IN || DEFAULT_TOKEN_EXPIRY;
            const token = jwt.sign(payload, process.env.JWT_SECRET || '', { expiresIn });

            res.cookie('jwt', token, {
                secure: process.env.NODE_ENV !== 'dev',
                httpOnly: true,
                domain
            });

            res.redirect(returnURL);
        });
    })(req, res, next);
};

export const loginOneLogin: RequestHandler = (req, res, next) => {
    logger.debug('attempting to authenticate with one-login...');

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
            const expiresIn = process.env.JWT_EXPIRES_IN || DEFAULT_TOKEN_EXPIRY;
            const token = jwt.sign(payload, process.env.JWT_SECRET || '', { expiresIn });

            res.cookie('jwt', token, {
                secure: process.env.NODE_ENV !== 'dev',
                httpOnly: true,
                domain
            });

            res.redirect(returnURL);
        });
    })(req, res, next);
};
