import { Readable } from 'node:stream';

import { Request, Response, NextFunction, Router } from 'express';
import { parse, stringify } from 'csv';
import multer from 'multer';
import { pick } from 'lodash';

import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { DataLakeService } from '../services/datalake';
import { translatableMetadataKeys } from '../types/translatable-metadata';
import { RelatedLink } from '../dtos/related-link-dto';

import { loadDataset } from './dataset';

export const translationRouter = Router();

const upload = multer({ storage: multer.memoryStorage() });

// imported translation filename can be constant as we overwrite each time it's imported
const TRANSLATION_FILENAME = 'translation-import.csv';

const collectTranslations = (dataset: Dataset, includeIds = false): TranslationDTO[] => {
    const revision = dataset.draftRevision!;
    const metadataEN = revision.metadata?.find((meta) => meta.language.includes('en'));
    const metadataCY = revision.metadata?.find((meta) => meta.language.includes('cy'));

    // ignore roundingDescription if rounding isn't applied
    const metadataKeys = translatableMetadataKeys.filter((key) => {
        return revision.roundingApplied === true ? true : key !== 'roundingDescription';
    });

    const translations: TranslationDTO[] = [
        ...dataset.dimensions?.map((dimension) => {
            const factTableColumn = dimension.factTableColumn;
            const dimMetaEN = dimension.metadata?.find((meta) => meta.language.includes('en'));
            const dimMetaCY = dimension.metadata?.find((meta) => meta.language.includes('cy'));
            const dimNameEN = dimMetaEN?.name === factTableColumn ? '' : dimMetaEN?.name;
            const dimNameCY = dimMetaCY?.name === factTableColumn ? '' : dimMetaCY?.name;

            return {
                type: 'dimension',
                key: dimension.factTableColumn,
                english: dimNameEN,
                cymraeg: dimNameCY,
                id: dimension.id
            };
        }),
        ...metadataKeys.map((prop) => ({
            type: 'metadata',
            key: prop,
            english: metadataEN?.[prop] as string,
            cymraeg: metadataCY?.[prop] as string
        })),
        ...(revision.relatedLinks || []).map((link: RelatedLink) => ({
            type: 'link',
            key: link.id,
            english: link.labelEN,
            cymraeg: link.labelCY
        }))
    ];

    return includeIds ? translations : translations.map((row) => pick(row, ['type', 'key', 'english', 'cymraeg']));
};

const parseUploadedTranslations = async (fileBuffer: Buffer): Promise<TranslationDTO[]> => {
    const translations: TranslationDTO[] = [];

    const csvParser: AsyncIterable<TranslationDTO> = Readable.from(fileBuffer).pipe(
        parse({ bom: true, columns: true, skip_records_with_empty_values: true })
    );

    for await (const row of csvParser) {
        translations.push(row);
    }

    return translations;
};

translationRouter.get(
    '/:dataset_id/preview',
    loadDataset({ draftRevision: { metadata: true }, dimensions: { metadata: true } }),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            logger.info('Previewing translations for export...');
            const dataset: Dataset = res.locals.dataset;
            const translations = collectTranslations(dataset, true);
            res.json(translations);
        } catch (error) {
            logger.error(error, 'Error previewing translations');
            next(new UnknownException());
        }
    }
);

translationRouter.get(
    '/:dataset_id/export',
    loadDataset({ draftRevision: { metadata: true }, dimensions: { metadata: true } }),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            logger.info('Exporting translations to CSV...');
            const dataset: Dataset = res.locals.dataset;
            const translations = collectTranslations(dataset);
            res.setHeader('Content-Type', 'text/csv');
            stringify(translations, { bom: true, header: true, quoted_string: true }).pipe(res);
        } catch (error) {
            logger.error(error, 'Error exporting translations');
            next(new UnknownException());
        }
    }
);

translationRouter.post(
    '/:dataset_id/import',
    upload.single('csv'),
    loadDataset({ draftRevision: { metadata: true }, dimensions: { metadata: true } }),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset: Dataset = res.locals.dataset;
        logger.info('Validating imported translations CSV...');

        if (!req.file || !req.file.buffer) {
            next(new BadRequestException('errors.upload.no_csv'));
            return;
        }

        try {
            // check the csv has all the keys and values required
            const existingTranslations = collectTranslations(dataset);
            const newTranslations = await parseUploadedTranslations(req.file.buffer);

            // validate the translation import is what we're expecting
            if (existingTranslations.length !== newTranslations.length) {
                next(new BadRequestException('errors.translation_file.invalid.row_count'));
                return;
            }

            existingTranslations.forEach((oldTranslation) => {
                const newTranslation = newTranslations.find(
                    (t) => oldTranslation.type === t.type && oldTranslation.key === t.key
                );

                if (!newTranslation) {
                    throw new BadRequestException('errors.translation_file.invalid.keys');
                }

                if (!newTranslation.english?.trim() || !newTranslation.cymraeg?.trim()) {
                    throw new BadRequestException('errors.translation_file.invalid.values');
                }
            });

            // store the translation import in the datalake so we can use it once it's confirmed as correct
            const datalake = new DataLakeService();
            await datalake.uploadFileBuffer(TRANSLATION_FILENAME, dataset.id, Buffer.from(req.file.buffer));

            res.status(201);
            res.json(DatasetDTO.fromDataset(dataset));
        } catch (error) {
            if (error instanceof BadRequestException) {
                next(error);
                return;
            }
            logger.error(error, 'Error importing translations');
            next(new UnknownException());
        }
    }
);

translationRouter.patch(
    '/:dataset_id/import',
    loadDataset({ draftRevision: { metadata: true }, dimensions: { metadata: true } }),
    async (req: Request, res: Response, next: NextFunction) => {
        let dataset: Dataset = res.locals.dataset;
        logger.info('Updating translations from CSV...');

        try {
            const datalake = new DataLakeService();
            const fileBuffer = await datalake.getFileBuffer(TRANSLATION_FILENAME, dataset.id);
            const newTranslations = await parseUploadedTranslations(fileBuffer);
            dataset = await req.datasetService.updateTranslations(dataset.id, newTranslations);
            await datalake.deleteFile(TRANSLATION_FILENAME, dataset.id);

            res.status(201);
            res.json(DatasetDTO.fromDataset(dataset));
        } catch (error) {
            logger.error(error, 'Error updating translations');
            next(new UnknownException());
        }
    }
);
