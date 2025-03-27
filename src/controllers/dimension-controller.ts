import { NextFunction, Request, Response } from 'express';

import { Dimension } from '../entities/dataset/dimension';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { DimensionType } from '../enums/dimension-type';
import { logger } from '../utils/logger';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DimensionDTO } from '../dtos/dimension-dto';
import { LookupTable } from '../entities/dataset/lookup-table';
import { getLatestRevision } from '../utils/latest';
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

export const getDimensionInfo = async (req: Request, res: Response) => {
  res.json(DimensionDTO.fromDimension(res.locals.dimension));
};

export const resetDimension = async (req: Request, res: Response) => {
  const dimension = res.locals.dimension;

  dimension.type = DimensionType.Raw;
  dimension.extractor = null;
  if (dimension.lookuptable) {
    const lookupTable: LookupTable = dimension.lookupTable;
    await lookupTable.remove();
    dimension.lookuptable = null;
  }
  await dimension.save();
  const updatedDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  res.status(202);
  res.json(DimensionDTO.fromDimension(updatedDimension));
};

export const sendDimensionPreview = async (req: Request, res: Response) => {
  const { dataset, dimension } = res.locals;
  const latestRevision = getLatestRevision(dataset);

  logger.debug(`Latest revision is ${JSON.stringify(latestRevision)}`);
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
};

export const attachLookupTableToDimension = async (req: Request, res: Response, next: NextFunction) => {
  if (!req.file) {
    next(new BadRequestException('errors.upload.no_csv'));
    return;
  }
  const { dataset, dimension } = res.locals;
  const language = req.language.toLowerCase();

  const { dataTable, buffer } = await validateAndUploadCSV(
    req.file.buffer,
    req.file?.mimetype,
    req.file?.originalname,
    res.locals.datasetId
  );

  const tableMatcher = req.body as LookupTablePatchDTO;

  try {
    const result = await validateLookupTable(dataTable, dataset, dimension, buffer, language, tableMatcher);
    if ((result as ViewErrDTO).status) {
      const error = result as ViewErrDTO;
      res.status(error.status);
      res.json(result);
      return;
    }
    res.status(200);
    res.json(result);
  } catch (err) {
    logger.error(`An error occurred trying to handle the lookup table: ${err}`);
    next(new UnknownException('errors.upload_error'));
  }
};

export const updateDimension = async (req: Request, res: Response) => {
  const { dataset, dimension } = res.locals;
  const language = req.language.toLowerCase();

  const dimensionPatchRequest = req.body as DimensionPatchDto;
  let preview: ViewDTO | ViewErrDTO;

  logger.debug(`User dimension type = ${JSON.stringify(dimensionPatchRequest)}`);
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
      preview = viewErrorGenerators(400, dataset.id, 'dimension_type', 'errors.dimension_validation.unknown_type', {});
  }

  if ((preview as ViewErrDTO).errors) {
    res.status((preview as ViewErrDTO).status);
  } else {
    res.status(202);
  }
  res.json(preview);
};

export const updateDimensionMetadata = async (req: Request, res: Response) => {
  const { dimension } = res.locals;

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
  res.status(202);
  res.json(DimensionDTO.fromDimension(updatedDimension));
};
