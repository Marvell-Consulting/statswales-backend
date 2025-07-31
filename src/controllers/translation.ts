import { createReadStream, ReadStream } from 'node:fs';
import { Readable } from 'node:stream';

import { Request, Response, NextFunction } from 'express';
import { parse, stringify } from 'csv';

import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { DatasetDTO } from '../dtos/dataset-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { EventLog } from '../entities/event-log';
import { collectTranslations } from '../utils/collect-translations';
import { DatasetRepository, withMetadataForTranslation } from '../repositories/dataset';
import { TempFile } from '../interfaces/temp-file';
import { uploadAvScan } from '../services/virus-scanner';
import { createAllCubeFiles } from '../services/cube-handler';

// imported translation filename can be constant as we overwrite each time it's imported
const TRANSLATION_FILENAME = 'translation-import.csv';

const parseTranslationsFromStream = async (stream: Readable | ReadStream): Promise<TranslationDTO[]> => {
  const translations: TranslationDTO[] = [];
  const csvParser = parse({ bom: true, columns: true, trim: true, skip_records_with_empty_values: true });

  return new Promise((resolve, reject) => {
    stream
      .pipe(csvParser)
      .on('data', (row) => translations.push(row as TranslationDTO))
      .on('error', (error: Error) => {
        logger.error(error, 'Error parsing translations CSV');
        reject(new BadRequestException('errors.translation_file.invalid.format'));
      })
      .on('end', () => resolve(translations));
  });
};

export const translationPreview = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('Previewing translations for export...');
    const dataset = await DatasetRepository.getById(res.locals.datasetId, withMetadataForTranslation);
    const translations = collectTranslations(dataset, true);
    res.json(translations);
  } catch (error) {
    logger.error(error, 'Error previewing translations');
    next(new UnknownException());
  }
};

export const translationExport = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('Exporting translations to CSV...');
    const dataset = await DatasetRepository.getById(res.locals.datasetId, withMetadataForTranslation);
    const revision = dataset.draftRevision!;
    const translations = collectTranslations(dataset);

    await EventLog.getRepository().save({
      action: 'export',
      entity: 'translations',
      entityId: revision.id,
      data: translations,
      userId: req.user?.id,
      client: 'sw3-frontend'
    });

    res.setHeader('Content-Type', 'text/csv');
    stringify(translations, { bom: true, header: true, quoted_string: true }).pipe(res);
  } catch (error) {
    logger.error(error, 'Error exporting translations');
    next(new UnknownException());
  }
};

export const validateImport = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.info('Validating imported translations CSV...');
  let tmpFile: TempFile;

  try {
    tmpFile = await uploadAvScan(req);
  } catch (err) {
    logger.error(err, 'There was a problem uploading the translation file');
    next(err);
    return;
  }

  try {
    const dataset = await DatasetRepository.getById(res.locals.datasetId, withMetadataForTranslation);
    const fileStream = createReadStream(tmpFile.path);

    fileStream.on('error', (error) => {
      logger.error(error, 'Error reading the uploaded translation file');
      next(new BadRequestException('errors.translation_file.invalid.file'));
      return;
    });

    // check the csv has all the keys and values required
    const existingTranslations = collectTranslations(dataset);
    const newTranslations = await parseTranslationsFromStream(fileStream);
    fileStream.destroy(); // close the stream after parsing

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
    const uploadStream = createReadStream(tmpFile.path);
    await req.fileService.saveStream(TRANSLATION_FILENAME, dataset.id, uploadStream);
    uploadStream.destroy(); // close the stream after uploading

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
};

export const applyImport = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.info('Updating translations from CSV...');
  const datasetId = res.locals.datasetId;

  try {
    const fileStream = await req.fileService.loadStream(TRANSLATION_FILENAME, datasetId);
    const newTranslations = await parseTranslationsFromStream(fileStream);
    const dataset = await req.datasetService.updateTranslations(datasetId, newTranslations);
    await req.fileService.delete(TRANSLATION_FILENAME, dataset.id);

    await EventLog.getRepository().save({
      action: 'import',
      entity: 'translations',
      entityId: dataset.draftRevisionId,
      data: newTranslations,
      userId: req.user?.id,
      client: 'sw3-frontend'
    });
    try {
      await createAllCubeFiles(dataset.id, dataset.draftRevisionId!);
    } catch (error) {
      logger.error(error, 'Error rebuilding cube after translations applied');
      next(new UnknownException('errors.cube_validation.failed'));
      return;
    }

    res.status(201);
    res.json(DatasetDTO.fromDataset(dataset));
  } catch (error) {
    logger.error(error, 'Error updating translations');
    next(new UnknownException());
  }
};
