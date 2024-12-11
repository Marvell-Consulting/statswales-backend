import { Request, Response, NextFunction, Router } from 'express';
import { stringify } from 'csv';

import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { TranslationDTO } from '../dtos/translations-dto';

import { loadDataset } from './dataset';

const collectTranslations = (dataset: Dataset): TranslationDTO[] => {
    const metadataEN = dataset.datasetInfo?.find((info) => info.language.includes('en'));
    const metadataCY = dataset.datasetInfo?.find((info) => info.language.includes('cy'));

    const metadataProps: (keyof DatasetInfo)[] = [
        'title',
        'description',
        'collection',
        'quality',
        'roundingDescription'
    ];

    const translations: TranslationDTO[] = [
        ...dataset.dimensions?.map((dim) => ({
            type: 'dimension',
            id: dim.id,
            key: dim.factTableColumn,
            english: dim.dimensionInfo?.find((info) => info.language.includes('en'))?.name,
            welsh: dim.dimensionInfo?.find((info) => info.language.includes('cy'))?.name
        })),
        ...metadataProps.map((prop) => ({
            type: 'metadata',
            key: prop,
            english: metadataEN?.[prop] as string,
            welsh: metadataCY?.[prop] as string
        }))
    ];

    return translations;
};

export const translationRouter = Router();

translationRouter.get(
    '/:dataset_id/preview',
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            logger.info('Previewing translations for export...');
            const dataset: Dataset = res.locals.dataset;
            const translations = collectTranslations(dataset);
            res.json(translations);
        } catch (error) {
            logger.error('Error previewing translations', error);
            next(new UnknownException());
        }
    }
);

translationRouter.get('/:dataset_id/export', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    try {
        logger.info('Exporting translations to CSV...');
        const dataset: Dataset = res.locals.dataset;
        const translations = collectTranslations(dataset);
        res.setHeader('Content-Type', 'text/csv');
        stringify(translations, { bom: true, header: true }).pipe(res);
    } catch (error) {
        logger.error('Error exporting translations', error);
        next(new UnknownException());
    }
});

translationRouter.get('/:dataset_id/import', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    try {
        logger.info('Importing translations from CSV...');

        // extract the translations from the CSV file and update where neccessary

        res.json({});
    } catch (error) {
        logger.error('Error importing translations', error);
        next(new UnknownException());
    }
});
