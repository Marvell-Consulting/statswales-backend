import { NextFunction, Request, Response } from 'express';

import { LookupTable } from '../entities/dataset/lookup-table';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { getMeasurePreview, validateMeasureLookupTable } from '../services/measure-handler';
import { validateAndUpload } from '../services/csv-processor';
import { DimensionMetadataDTO } from '../dtos/dimension-metadata-dto';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';
import { LookupTableDTO } from '../dtos/lookup-table-dto';
import { Readable } from 'node:stream';
import { MeasureDTO } from '../dtos/measure-dto';
import { DatasetRepository } from '../repositories/dataset';
import { createAllCubeFiles } from '../services/cube-handler';

export const resetMeasure = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = res.locals.dataset;
  const measure = dataset.measure;
  if (!measure) {
    next(new NotFoundException('errors.measure_missing'));
    return;
  }
  logger.debug('Resetting measure by removing extractor, lookup table, info and join column');
  measure.extractor = null;
  if (measure.lookup) {
    const measureLookupFilename = measure.lookup.filename;
    const lookupTable: LookupTable = measure.lookupTable;
    await lookupTable.remove();
    measure.lookupTable = null;
    logger.debug(`Removing file ${dataset.id}/${measureLookupFilename} from data lake`);
    await req.fileService.delete(measureLookupFilename, dataset.id);
  }
  if (measure.measureInfo) {
    logger.debug('Removing all measure info');
    for (const info of measure.measureInfo) {
      await info.remove();
    }
  }
  measure.joinColumn = null;
  logger.debug('Saving measure and returning dataset');
  await measure.save();
  await createAllCubeFiles(dataset.id, dataset.draftRevision!.id);
  const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
  const dto = DatasetDTO.fromDataset(updateDataset);
  res.json(dto);
};

export const attachLookupTableToMeasure = async (req: Request, res: Response, next: NextFunction) => {
  if (!req.file) {
    next(new BadRequestException('errors.upload.no_csv'));
    return;
  }

  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    factTable: true,
    measure: { metadata: true, measureTable: true, lookupTable: true },
    draftRevision: { dataTable: true }
  });

  try {
    const { dataTable, buffer } = await validateAndUpload(req.file, dataset.id, 'lookup_table');
    const lang = req.language.toLowerCase();
    const tableMatcher = req.body as MeasureLookupPatchDTO;
    const result = await validateMeasureLookupTable(dataTable, dataset, buffer, lang, tableMatcher);
    await createAllCubeFiles(dataset.id, dataset.draftRevision!.id);
    res.status((result as ViewErrDTO).status || 200);

    // logger.debug(`Result of the lookup table validation is ${JSON.stringify(result, null, 2)}`);
    res.json(result);
  } catch (err) {
    await createAllCubeFiles(dataset.id, dataset.draftRevision!.id);
    logger.error(err, `An error occurred trying to process and upload the file`);
    next(new UnknownException('errors.upload_error'));
  }
};

export const getPreviewOfMeasure = async (req: Request, res: Response, next: NextFunction) => {
  const lang = req.language.toLowerCase();

  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    factTable: true,
    measure: { metadata: true, measureTable: true, lookupTable: true },
    draftRevision: { dataTable: { dataTableDescriptions: true } },
    revisions: { dataTable: { dataTableDescriptions: true } }
  });

  if (!dataset.measure) {
    next(new NotFoundException('errors.measure_invalid'));
    return;
  }

  try {
    const preview = await getMeasurePreview(dataset, lang);
    res.json(preview);
  } catch (err) {
    logger.error(err, `Something went wrong trying to get a preview of the measure`);
    next(new UnknownException('errors.upload_error'));
  }
};

export const updateMeasureMetadata = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    measure: { metadata: true }
  });

  const measure = dataset.measure;
  if (!measure) {
    next(new NotFoundException('errors.measure_invalid'));
  }

  const update = req.body as DimensionMetadataDTO;
  let metadata = measure.metadata.find((meta: MeasureMetadata) => meta.language === update.language);

  if (!metadata) {
    metadata = new MeasureMetadata();
    metadata.measure = measure;
    metadata.language = update.language;
  }

  if (update.name) {
    metadata.name = update.name;
  }

  if (update.notes) {
    metadata.notes = update.notes;
  }

  const updatedMeasureMetadata = await metadata.save();

  res.json(DimensionMetadataDTO.fromDimensionMetadata(updatedMeasureMetadata));
};

export const getMeasureInfo = async (req: Request, res: Response) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    measure: { metadata: true, measureTable: true, lookupTable: true }
  });

  if (!dataset.measure) {
    res.status(404);
    res.json({ message: 'No measure found' });
    return;
  }

  res.json(MeasureDTO.fromMeasure(dataset.measure));
};

export const getMeasureLookupTableInfo = async (req: Request, res: Response) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    measure: { lookupTable: true }
  });

  if (!dataset.measure?.lookupTable) {
    res.status(404);
    res.json({ message: 'No lookup table found' });
    return;
  }

  res.json(LookupTableDTO.fromLookupTable(dataset.measure.lookupTable));
};

export const downloadMeasureLookupTable = async (req: Request, res: Response) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    measure: { lookupTable: true }
  });

  if (!dataset.measure?.lookupTable) {
    res.status(404);
    res.json({ message: 'No lookup table found' });
    return;
  }

  const lookupTable = dataset.measure.lookupTable;
  const filename = lookupTable.originalFilename || lookupTable.filename;
  let stream: Readable;

  try {
    stream = await req.fileService.loadStream(lookupTable.filename, dataset.id);
  } catch (err) {
    logger.error(err, `An error occurred trying to load the file ${filename} from the data lake`);
    res.status(500);
    res.json({ message: 'An error occurred trying to load the file' });
    return;
  }

  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': `${lookupTable.mimeType}`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Disposition': `attachment; filename=${filename}`
  });
  stream.pipe(res);

  // Handle errors in the file stream
  stream.on('error', (err) => {
    logger.error('File stream error:', err);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Server Error');
  });

  // Optionally listen for the end of the stream
  stream.on('end', () => {
    logger.debug('File stream ended');
  });
};
