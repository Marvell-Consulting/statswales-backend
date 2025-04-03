import { Readable } from 'node:stream';

import { Request, Response, NextFunction, Router } from 'express';
import { parse, stringify } from 'csv';
import multer from 'multer';

import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { EventLog } from '../entities/event-log';

import { loadDataset } from './dataset';
import { collectTranslations } from '../utils/collect-translations';

export const translationRouter = Router();

const upload = multer({ storage: multer.memoryStorage() });

// imported translation filename can be constant as we overwrite each time it's imported
const TRANSLATION_FILENAME = 'translation-import.csv';

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
      const revision = dataset.draftRevision!;
      const translations = collectTranslations(dataset);

      await EventLog.getRepository().save({
        action: 'export',
        entity: 'translations',
        entityId: revision.id,
        data: { translations },
        userId: req.user?.id,
        client: 'sw3-frontend'
      });

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

      // store the translation import in the file store so we can use it once it's confirmed as correct
      await req.fileService.saveBuffer(TRANSLATION_FILENAME, dataset.id, Buffer.from(req.file.buffer));

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
    const revision = dataset.draftRevision!;
    logger.info('Updating translations from CSV...');

    try {
      const fileBuffer = await req.fileService.loadBuffer(TRANSLATION_FILENAME, dataset.id);
      const newTranslations = await parseUploadedTranslations(fileBuffer);
      dataset = await req.datasetService.updateTranslations(dataset.id, newTranslations);
      await req.fileService.delete(TRANSLATION_FILENAME, dataset.id);

      await EventLog.getRepository().save({
        action: 'import',
        entity: 'translations',
        entityId: revision.id,
        data: newTranslations,
        userId: req.user?.id,
        client: 'sw3-frontend'
      });

      res.status(201);
      res.json(DatasetDTO.fromDataset(dataset));
    } catch (error) {
      logger.error(error, 'Error updating translations');
      next(new UnknownException());
    }
  }
);
