import { Router } from 'express';
import passport from 'passport';
import { generators, AuthorizationParameters } from 'openid-client';

import { loginGoogle, loginOneLogin } from '../controllers/auth';

export const auth = Router();

auth.get('/google', passport.authenticate('google'));
auth.get('/google/callback', loginGoogle);

auth.get('/onelogin', (req, res, next) => {
    const params: AuthorizationParameters = { nonce: generators.nonce() };
    passport.authenticate('onelogin', params)(req, res, next);
});
auth.get('/onelogin/callback', loginOneLogin);
