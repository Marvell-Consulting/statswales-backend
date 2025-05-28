import { Readable } from 'node:stream';

import { NextFunction, Request, Response } from 'express';

import { Dimension } from '../entities/dataset/dimension';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { DimensionType } from '../enums/dimension-type';
import { logger } from '../utils/logger';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DimensionDTO } from '../dtos/dimension-dto';
import { LookupTable } from '../entities/dataset/lookup-table';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';
import { DimensionMetadataDTO } from '../dtos/dimension-metadata-dto';
import { getFactTableColumnPreview, validateAndUploadCSV } from '../services/csv-processor';
import {
  getDimensionPreview,
  setupTextDimension,
  createAndValidateDateDimension,
  validateNumericDimension
} from '../services/dimension-processor';
import { validateLookupTable } from '../services/lookup-table-handler';
import { validateReferenceData } from '../services/reference-data-handler';
import { viewErrorGenerators } from '../utils/view-error-generators';
import { LookupTableDTO } from '../dtos/lookup-table-dto';
import { DatasetRepository } from '../repositories/dataset';
import { getLatestRevision } from '../utils/latest';
import { Dataset } from '../entities/dataset/dataset';
import { createBaseCubeFromProtoCube } from '../services/cube-handler';
import { getFileService } from '../utils/get-file-service';

export const getDimensionInfo = async (req: Request, res: Response) => {
  res.json(DimensionDTO.fromDimension(res.locals.dimension));
};

export const resetDimension = async (req: Request, res: Response) => {
  const dimension = res.locals.dimension;

  dimension.type = DimensionType.Raw;
  dimension.extractor = null;

  if (dimension.lookuptable) {
    const lookupTable: LookupTable = dimension.lookupTable;
    logger.debug(`Removing previously uploaded lookup table from dimension`);
    try {
      const fileService = getFileService();
      await fileService.delete(lookupTable.filename, dimension.dataset.id);
    } catch (err) {
      logger.warn(`Something went wrong trying to remove previously uploaded lookup table with error: ${err}`);
    }
    await lookupTable.remove();
    dimension.lookuptable = null;
  }

  await dimension.save();
  const updatedDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  res.status(202);
  res.json(DimensionDTO.fromDimension(updatedDimension));
};

export const sendDimensionPreview = async (req: Request, res: Response, next: NextFunction) => {
  const dimension = res.locals.dimension;
  let dataset: Dataset;

  try {
    dataset = await DatasetRepository.getById(res.locals.datasetId, {
      factTable: true,
      draftRevision: { dataTable: { dataTableDescriptions: true } },
      revisions: { dataTable: { dataTableDescriptions: true } }
    });

    const latestRevision = getLatestRevision(dataset);
    logger.debug(`Latest revision is ${latestRevision?.id}`);

    if (latestRevision?.tasks) {
      const outstandingDimensionTask = latestRevision.tasks.dimensions.find((dim) => dim.id === dimension.id);
      if (outstandingDimensionTask && !outstandingDimensionTask.lookupTableUpdated) {
        dimension.type = DimensionType.Raw;
      }
    }

    let preview: ViewDTO | ViewErrDTO;
    if (dimension.type === DimensionType.Raw) {
      preview = await getFactTableColumnPreview(dataset, dimension.factTableColumn);
    } else {
      preview = await getDimensionPreview(dataset, dimension, req.language);
    }

    if ((preview as ViewErrDTO).errors) {
      res.status(500);
    }

    res.json(preview);
  } catch (err) {
    logger.error(err, `An error occurred trying to load the dimension preview`);
    next(new UnknownException('errors.dimension_preview'));
  }
};

export const attachLookupTableToDimension = async (req: Request, res: Response, next: NextFunction) => {
  if (!req.file) {
    next(new BadRequestException('errors.upload.no_csv'));
    return;
  }

  const dimension = res.locals.dimension;
  const language = req.language.toLowerCase();

  try {
    const dataset = await DatasetRepository.getById(res.locals.datasetId, {
      factTable: true,
      draftRevision: { dataTable: { dataTableDescriptions: true } },
      revisions: { dataTable: { dataTableDescriptions: true } }
    });

    const { dataTable, buffer } = await validateAndUploadCSV(
      req.file.buffer,
      req.file?.mimetype,
      req.file?.originalname,
      res.locals.datasetId,
      'lookup_table'
    );

    const tableMatcher = req.body as LookupTablePatchDTO;

    const result = await validateLookupTable(dataTable, dataset, dimension, buffer, language, tableMatcher);
    await createBaseCubeFromProtoCube(dataset.id, dataset.draftRevision!.id);
    if ((result as ViewErrDTO).status) {
      const error = result as ViewErrDTO;
      res.status(error.status);
      res.json(result);
      return;
    }

    res.json(result);
  } catch (err) {
    logger.error(err, `An error occurred trying to handle the lookup table`);
    next(new UnknownException('errors.upload_error'));
  }
};

export const updateDimension = async (req: Request, res: Response, next: NextFunction) => {
  const dimension = res.locals.dimension;
  const language = req.language.toLowerCase();
  const dimensionPatchRequest = req.body as DimensionPatchDto;
  let preview: ViewDTO | ViewErrDTO;

  logger.debug(`User dimension type = ${JSON.stringify(dimensionPatchRequest)}`);

  try {
    const dataset = await DatasetRepository.getById(res.locals.datasetId, {
      factTable: true,
      draftRevision: { dataTable: { dataTableDescriptions: true } },
      revisions: { dataTable: { dataTableDescriptions: true } }
    });

    switch (dimensionPatchRequest.dimension_type) {
      case DimensionType.DatePeriod:
      case DimensionType.Date:
        logger.debug('Matching a Dimension containing Dates');
        preview = await createAndValidateDateDimension(dimensionPatchRequest, dataset, dimension, language);
        break;

      case DimensionType.ReferenceData:
        logger.debug('Matching a Dimension containing Reference Data');
        preview = await validateReferenceData(
          dataset,
          dimension,
          dimensionPatchRequest.reference_type,
          `${req.language}`
        );
        break;

      case DimensionType.Text:
        await setupTextDimension(dimension);
        preview = await getFactTableColumnPreview(dataset, dimension.factTableColumn);
        break;

      case DimensionType.Numeric:
        preview = await validateNumericDimension(dimensionPatchRequest, dataset, dimension);
        break;

      case DimensionType.LookupTable:
        logger.debug('User requested to patch a lookup table?');
        preview = viewErrorGenerators(
          400,
          dataset.id,
          'dimension_type',
          'errors.dimension_validation.lookup_not_supported',
          {}
        );
        break;

      default:
        preview = viewErrorGenerators(
          400,
          dataset.id,
          'dimension_type',
          'errors.dimension_validation.unknown_type',
          {}
        );
    }
    try {
      await createBaseCubeFromProtoCube(dataset.id, dataset.draftRevision!.id);
    } catch (error) {
      logger.error(error, `An error occurred trying to create a base cube`);
      res.status(500);
      res.json(
        viewErrorGenerators(500, dataset.id, 'dimension_type', 'errors.dimension_validation.cube_creation_failed', {})
      );
      return;
    }

    if ((preview as ViewErrDTO).errors) {
      res.status((preview as ViewErrDTO).status);
    } else {
      res.status(202);
    }
    res.json(preview);
  } catch (err) {
    logger.error(err, `An error occurred trying to update the dimension`);
    next(new UnknownException('errors.dimension_update'));
  }
};

export const updateDimensionMetadata = async (req: Request, res: Response) => {
  const dimension = res.locals.dimension;
  const dataset = await DatasetRepository.getById(res.locals.datasetId, {
    draftRevision: { dataTable: { dataTableDescriptions: true } }
  });
  const update = req.body as DimensionMetadataDTO;
  let metadata = dimension.metadata.find((meta: DimensionMetadata) => meta.language === update.language);

  if (!metadata) {
    metadata = new DimensionMetadata();
    metadata.dimension = dimension;
    metadata.language = update.language;
  }
  if (update.name) {
    metadata.name = update.name;
  }
  if (update.notes) {
    metadata.notes = update.notes;
  }
  await metadata.save();
  const updatedDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  console.log(`dataset id = ${dataset?.id} and revision id = ${dataset.draftRevision?.id}`);
  await createBaseCubeFromProtoCube(dataset.id, dataset.draftRevision!.id);
  res.status(202);
  res.json(DimensionDTO.fromDimension(updatedDimension));
};

export const getDimensionLookupTableInfo = async (req: Request, res: Response) => {
  const lookupTable = res.locals.dimension?.lookupTable;
  if (!lookupTable) {
    res.status(404);
    res.json({ message: 'No lookup table found' });
    return;
  }
  res.json(LookupTableDTO.fromLookupTable(lookupTable));
};

export const downloadDimensionLookupTable = async (req: Request, res: Response) => {
  const { dataset, dimension } = res.locals;
  const lookupTable: LookupTable = dimension.lookupTable;

  if (!lookupTable) {
    res.status(404);
    res.json({ message: 'No lookup table found' });
    return;
  }

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
