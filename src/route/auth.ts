import { Router } from 'express';
import passport from 'passport';

import { loginGoogle, loginOneLogin } from '../services/auth';

export const auth = Router();

auth.get('/google', passport.authenticate('google'));
auth.get('/google/callback', loginGoogle);

auth.get('/onelogin', passport.authenticate('onelogin'));
auth.get('/onelogin/callback', loginOneLogin);
