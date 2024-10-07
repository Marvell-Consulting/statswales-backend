import { Router } from 'express';
import passport from 'passport';

import { sanitiseUser } from '../utils/sanitise-user';
import { User } from '../entities/user';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { appConfig } from '../config';
import { AppEnv } from '../config/env.enum';
import { DataLakeService } from '../controllers/datalake';
import { logger } from '../utils/logger';

const config = appConfig();

const healthcheck = Router();

healthcheck.get('/', (req, res) => {
    res.json({ message: 'success' });
});

healthcheck.get('/basic', (req, res) => {
    res.json({ message: 'success' });
});

healthcheck.get('/language', (req, res) => {
    res.json({ lang: req.language, supported: SUPPORTED_LOCALES });
});

healthcheck.get('/jwt', passport.authenticate('jwt', { session: false }), (req, res) => {
    res.json({ message: 'success', user: sanitiseUser(req.user as User) });
});

if (config.env !== AppEnv.Ci) {
    healthcheck.get('/datalake', (req, res) => {
        try {
            const dataLakeService = new DataLakeService();
            dataLakeService.listFiles();
            res.json({ message: 'success' });
        } catch (err) {
            logger.error(`Unable to connect to datalake: ${err}`);
            res.status(500).json({ message: 'error' });
        }
    });
}

export const healthcheckRouter = healthcheck;
