import { Router } from 'express';
import passport from 'passport';

// import { logger } from '../utils/logger';
// import { DataLakeService } from '../controllers/datalake';
import { sanitiseUser } from '../utils/sanitise-user';
import { User } from '../entities/user';

const healthcheck = Router();

healthcheck.get('/basic', (req, res) => {
    res.json({ message: 'success' });
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
