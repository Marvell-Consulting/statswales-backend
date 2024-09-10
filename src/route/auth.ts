import { Router } from 'express';
import passport from 'passport';
import { generators, AuthorizationParameters } from 'openid-client';

import { loginGoogle, loginOneLogin } from '../controllers/auth';

const auth = Router();

if (process.env.AUTH_PROVIDERS?.includes('google')) {
    auth.get('/google', passport.authenticate('google'));
    auth.get('/google/callback', loginGoogle);
}

if (process.env.AUTH_PROVIDERS?.includes('onelogin')) {
    auth.get('/onelogin', (req, res, next) => {
        const params: AuthorizationParameters = { nonce: generators.nonce() };
        passport.authenticate('onelogin', params)(req, res, next);
    });
    auth.get('/onelogin/callback', loginOneLogin);
}

export const authRouter = auth;
