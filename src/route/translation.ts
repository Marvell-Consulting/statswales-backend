import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';

export const translationRouter = Router();

translationRouter.get('/preview', async (req: Request, res: Response, next: NextFunction) => {
    try {
        logger.info('Previewing translations for export...');

        // collect all the things needing translation
        // return it as a json object

        res.json({});
    } catch (error) {
        logger.error('Error previewing translations', error);
        next(new UnknownException());
    }
});

translationRouter.get('/export', async (req: Request, res: Response, next: NextFunction) => {
    try {
        logger.info('Exporting translations to CSV...');

        // collect all the things needing translation
        // return it as a CSV file

        res.json({});
    } catch (error) {
        logger.error('Error exporting translations', error);
        next(new UnknownException());
    }
});

translationRouter.get('/import', async (req: Request, res: Response, next: NextFunction) => {
    try {
        logger.info('Importing translations from CSV...');

        // extract the translations from the CSV file and update where neccessary

        res.json({});
    } catch (error) {
        logger.error('Error importing translations', error);
        next(new UnknownException());
    }
});
