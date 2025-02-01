import { NextFunction, Router, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { listPublishedDatasets, getPublishedDatasetById, downloadPublishedDataset } from '../controllers/consumer';
import { NotFoundException } from '../exceptions/not-found.exception';
import { DatasetRepository } from '../repositories/dataset';
import { hasError, datasetIdValidator } from '../validators';

export const consumerRouter = Router();

export const loadPublishedDataset = async (req: Request, res: Response, next: NextFunction) => {
    const datasetIdError = await hasError(datasetIdValidator(), req);

    if (datasetIdError) {
        logger.error(datasetIdError);
        next(new NotFoundException('errors.dataset_id_invalid'));
        return;
    }

    try {
        logger.debug(`Loading published dataset ${req.params.dataset_id}...`);
        const dataset = await DatasetRepository.getPublishedById(req.params.dataset_id);
        res.locals.datasetId = dataset.id;
        res.locals.dataset = dataset;
    } catch (err) {
        logger.error(`Failed to load dataset, error: ${err}`);
        next(new NotFoundException('errors.no_dataset'));
        return;
    }

    next();
};

// GET /published/list
// Returns a list of all active datasets e.g. ones with imports
consumerRouter.get('/list', listPublishedDatasets);

// GET /published/:datasetId
// Returns a published dataset as a json object
consumerRouter.get('/:dataset_id', loadPublishedDataset, getPublishedDatasetById);

// GET /published/:datasetId/revision/:revisionId/download/:format
// Returns a published dataset as a file stream
consumerRouter.get(
    '/:dataset_id/revision/:revision_id/download/:format',
    loadPublishedDataset,
    downloadPublishedDataset
);
