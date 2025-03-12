import {NextFunction, Request, Response} from 'express';

import {Dimension} from '../entities/dataset/dimension';
import {DimensionMetadata} from '../entities/dataset/dimension-metadata';
import {DimensionType} from '../enums/dimension-type';
import {DataTable} from '../entities/dataset/data-table';
import {logger} from '../utils/logger';
import {DimensionPatchDto} from '../dtos/dimension-partch-dto';
import {ViewDTO, ViewErrDTO} from '../dtos/view-dto';
import {NotFoundException} from '../exceptions/not-found.exception';
import {DimensionDTO} from '../dtos/dimension-dto';
import {LookupTable} from '../entities/dataset/lookup-table';
import {getLatestRevision} from '../utils/latest';
import {BadRequestException} from '../exceptions/bad-request.exception';
import {UnknownException} from '../exceptions/unknown.exception';
import {LookupTablePatchDTO} from '../dtos/lookup-patch-dto';
import {DimensionMetadataDTO} from '../dtos/dimension-metadata-dto';
import {getFactTableColumnPreview, uploadCSV} from '../services/csv-processor';
import {
  getDimensionPreview,
  setupTextDimension,
  validateDateTypeDimension,
  validateNumericDimension
} from '../services/dimension-processor';
import {validateLookupTable} from '../services/lookup-table-handler';
import {validateReferenceData} from '../services/reference-data-handler';
import {convertBufferToUTF8} from '../utils/file-utils';

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

export const sendDimensionPreview = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, dimension } = res.locals;
  const latestRevision = getLatestRevision(dataset);
  const dataTable = latestRevision?.dataTable;

  if (!dataTable) {
    next(new NotFoundException('errors.fact_table_invalid'));
    return;
  }

  logger.debug(`Latest revision is ${JSON.stringify(latestRevision)}`);
  if (latestRevision?.tasks) {
    const outstandingDimensionTask = latestRevision.tasks.dimensions.find((dim) => dim.id === dimension.id);
    if (outstandingDimensionTask && !outstandingDimensionTask.lookupTableUpdated) {
      dimension.type = DimensionType.Raw;
    }
  }
  try {
    let preview: ViewDTO | ViewErrDTO;
    if (dimension.type === DimensionType.Raw) {
      preview = await getFactTableColumnPreview(dataset, dataTable, dimension.factTableColumn);
    } else {
      preview = await getDimensionPreview(dataset, dimension, dataTable, req.language);
    }
    if ((preview as ViewErrDTO).errors) {
      res.status(500);
      res.json(preview);
    }
    res.status(200);
    res.json(preview);
  } catch (err) {
    logger.error(`Something went wrong trying to get a preview of the dimension with the following error: ${err}`);
    res.status(500);
    res.json({ message: 'Something went wrong trying to generate a preview of the dimension' });
  }
};

export const attachLookupTableToDimension = async (req: Request, res: Response, next: NextFunction) => {
  if (!req.file) {
    next(new BadRequestException('errors.upload.no_csv'));
    return;
  }
  const { dataset, dimension } = res.locals;

  const dataTable = getLatestRevision(dataset)?.dataTable;

  if (!dataTable) {
    next(new NotFoundException('errors.fact_table_invalid'));
    return;
  }

  let fileImport: DataTable;
  let utf8Buffer: Buffer<ArrayBufferLike>;
  switch (req.file.mimetype) {
    case 'text/csv':
    case 'application/csv':
    case 'application/json':
      utf8Buffer = convertBufferToUTF8(req.file.buffer);
      break;
    default:
      utf8Buffer = req.file.buffer;
  }

  try {
    fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, res.locals.datasetId);
  } catch (err) {
    logger.error(`An error occurred trying to upload the file: ${err}`);
    next(new UnknownException('errors.upload_error'));
    return;
  }

  const tableMatcher = req.body as LookupTablePatchDTO;

  try {
    const result = await validateLookupTable(fileImport, dataTable, dataset, dimension, utf8Buffer, tableMatcher);
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

export const updateDimension = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, dimension } = res.locals;
  const dataTable = getLatestRevision(dataset)?.dataTable;

  if (!dataTable) {
    next(new NotFoundException('errors.fact_table_invalid'));
    return;
  }

  const dimensionPatchRequest = req.body as DimensionPatchDto;
  let preview: ViewDTO | ViewErrDTO;

  try {
    logger.debug(`User dimension type = ${JSON.stringify(dimensionPatchRequest)}`);
    switch (dimensionPatchRequest.dimension_type) {
      case DimensionType.DatePeriod:
      case DimensionType.Date:
        logger.debug('Matching a Dimension containing Dates');
        preview = await validateDateTypeDimension(dimensionPatchRequest, dataset, dimension, dataTable);
        break;
      case DimensionType.ReferenceData:
        logger.debug('Matching a Dimension containing Reference Data');
        preview = await validateReferenceData(
          dataTable,
          dataset,
          dimension,
          dimensionPatchRequest.reference_type,
          `${req.language}`
        );
        break;
      case DimensionType.Text:
        await setupTextDimension(dimension);
        preview = await getFactTableColumnPreview(dataset, dataTable, dimension.factTableColumn);
        break;
      case DimensionType.Numeric:
        preview = await validateNumericDimension(dimensionPatchRequest, dataset, dataTable, dimension);
        break;
      case DimensionType.LookupTable:
        logger.debug('User requested to patch a lookup table?');
        throw new Error('You need to post a lookup table with this request');
      default:
        throw new Error('Not Implemented Yet!');
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to validate the dimension`);
    res.status(500);
    res.json({ message: 'Unable to validate or match dimension against patch' });
    return;
  }

  if ((preview as ViewErrDTO).errors) {
    res.status((preview as ViewErrDTO).status);
    res.json(preview);
    return;
  }

  res.status(200);
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
