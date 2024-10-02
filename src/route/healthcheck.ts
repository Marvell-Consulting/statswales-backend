import { Router } from 'express';
import passport from 'passport';

// import { logger } from '../utils/logger';
// import { DataLakeService } from '../controllers/datalake';
import { sanitiseUser } from '../utils/sanitise-user';
import { User } from '../entities/user';
import { AVAILABLE_LANGUAGES } from '../middleware/translation';

const healthcheck = Router();

healthcheck.get('/', (req, res) => {
    res.json({ message: 'success' });
});

healthcheck.get('/basic', (req, res) => {
    res.json({ message: 'success' });
});

healthcheck.get('/language', (req, res) => {
    res.json({ lang: req.language, supported: AVAILABLE_LANGUAGES });
});

healthcheck.get('/jwt', passport.authenticate('jwt', { session: false }), (req, res) => {
    res.json({ message: 'success', user: sanitiseUser(req.user as User) });
});

// healthcheck.get('/datalake', (req, res) => {
//     const dataLakeService = new DataLakeService();
//     try {
//         dataLakeService.listFiles();
//     } catch (err) {
//         logger.error(`Unable to connect to datalake: ${err}`);
//         res.status(500).json({ message: 'error' });
//         return;
//     }
//     res.json({ message: 'success' });
// });

export const healthcheckRouter = healthcheck;
