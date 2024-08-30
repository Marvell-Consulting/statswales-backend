import { RequestHandler } from 'express';
import passport from 'passport';
import jwt from 'jsonwebtoken';
import { pick } from 'lodash';

import { logger } from '../utils/logger';

export const loginGoogle: RequestHandler = (req, res, next) => {
    logger.debug('attempting to authenticate with google...');
    const returnURL = `${process.env.FRONTEND_URL}/auth/callback`;

    passport.authenticate('google', { session: false }, (err, user, info) => {
        if (err || !user) {
            logger.error(`google auth returned an error: ${info.message}`);
            res.redirect(`${returnURL}?error=provider`);
        }
        req.login(user, { session: false }, (error) => {
            if (error) {
                logger.error(`error logging in: ${error}`);
                res.redirect(`${returnURL}?error=login`);
            }

            logger.info('google auth successful, creating JWT and returning user to the frontend');

            const payload = { user: pick(user, ['id', 'email', 'firstName', 'lastName']) };
            const expiresIn = process.env.JWT_EXPIRES_IN || '1d';
            const token = jwt.sign(payload, process.env.JWT_SECRET || '', { expiresIn });

            res.cookie('jwt', token, {
                secure: process.env.NODE_ENV !== 'dev',
                httpOnly: true,
                domain: 'localhost'
            });

            res.redirect(returnURL);
        });
    })(req, res, next);
};

export const loginOneLogin: RequestHandler = (req, res, next) => {
    logger.debug('attempting to authenticate with one-login...');
    next();
};
