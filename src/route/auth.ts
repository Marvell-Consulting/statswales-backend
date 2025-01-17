import { Router } from 'express';
import passport from 'passport';

import { loginEntraID, loginGoogle } from '../controllers/auth';
import { appConfig } from '../config';
import { AuthProvider } from '../enums/auth-providers';

const config = appConfig();
const auth = Router();

if (config.auth.providers.includes(AuthProvider.Google)) {
    auth.get('/google', passport.authenticate(AuthProvider.Google, { prompt: 'select_account' }));
    auth.get('/google/callback', loginGoogle);
}

if (config.auth.providers.includes(AuthProvider.EntraId)) {
    auth.get('/entraid', passport.authenticate(AuthProvider.EntraId));
    auth.get('/entraid/callback', loginEntraID);
}

export const authRouter = auth;
