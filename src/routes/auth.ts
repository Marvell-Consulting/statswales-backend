import { Request, Response, Router } from 'express';
import passport from 'passport';

import { loginEntraID, loginLocal } from '../controllers/auth';
import { config } from '../config';
import { AuthProvider } from '../enums/auth-providers';

const auth = Router();

if (config.auth.providers.includes(AuthProvider.EntraId)) {
  auth.get('/entraid', passport.authenticate(AuthProvider.EntraId));
  auth.get('/entraid/callback', loginEntraID);
}

// Should only be used in testing environments
if (config.auth.providers.includes(AuthProvider.Local)) {
  auth.get('/local', loginLocal);
}

auth.get('/providers', (req: Request, res: Response) => {
  res.json({ enabled: config.auth.providers });
});

export const authRouter = auth;
