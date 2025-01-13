import { Request, Response, NextFunction } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { DatasetRepository } from '../repositories/dataset';

export const listPublishedDatasets = async (req: Request, res: Response, next: NextFunction) => {
    logger.info('Listing published datasets...');
    try {
        const lang = req.language as Locale;
        const page = parseInt(req.query.page as string, 10) || 1;
        const limit = parseInt(req.query.limit as string, 10) || 10;

        const results: ResultsetWithCount<DatasetListItemDTO> = await DatasetRepository.listPublishedByLanguage(
            lang,
            page,
            limit
        );

        res.json(results);
    } catch (err) {
        logger.error(err, 'Failed to fetch published dataset list');
        next(new UnknownException());
    }
};
