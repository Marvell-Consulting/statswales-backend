import { Router } from 'express';
import passport from 'passport';
import { generators, AuthorizationParameters } from 'openid-client';

import { loginGoogle, loginOneLogin } from '../controllers/auth';
import { appConfig } from '../config';

const config = appConfig();
const auth = Router();

if (config.auth.providers.includes('google')) {
    auth.get('/google', passport.authenticate('google', { prompt: 'select_account' }));
    auth.get('/google/callback', loginGoogle);
}

if (config.auth.providers.includes('onelogin')) {
    auth.get('/onelogin', (req, res, next) => {
        const params: AuthorizationParameters = { nonce: generators.nonce(), ui_locales: req.language };
        passport.authenticate('onelogin', params)(req, res, next);
    });
    auth.get('/onelogin/callback', loginOneLogin);
}

export const authRouter = auth;
