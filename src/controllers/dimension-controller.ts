import { NextFunction, Request, Response } from 'express';

import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { FactTable } from '../entities/dataset/fact-table';
import { logger } from '../utils/logger';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { NotFoundException } from '../exceptions/not-found.exception';
import { DimensionDTO } from '../dtos/dimension-dto';
import { LookupTable } from '../entities/dataset/lookup-table';
import { getLatestRevision } from '../utils/latest';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';
import { DimensionInfoDTO } from '../dtos/dimension-info-dto';
import { getFactTableColumnPreview, uploadCSV } from '../services/csv-processor';
import { getDimensionPreview, validateDateTypeDimension } from '../services/dimension-processor';
import { validateLookupTable } from '../services/lookup-table-handler';
import { validateReferenceData } from '../services/reference-data-handler';
import { convertBufferToUTF8 } from '../utils/file-utils';

export const getDimensionInfo = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);

    if (!dimension) {
        next(new NotFoundException('errors.dimension_id_invalid'));
        return;
    }

    res.json(DimensionDTO.fromDimension(dimension));
};

export const resetDimension = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);

    if (!dimension) {
        next(new NotFoundException('errors.dimension_id_invalid'));
        return;
    }
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
    const dataset = res.locals.dataset;
    const dimension: Dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);
    const factTable = getLatestRevision(dataset)?.factTables[0];
    if (!dimension) {
        next(new NotFoundException('errors.dimension_id_invalid'));
        return;
    }
    if (!factTable) {
        next(new NotFoundException('errors.fact_table_invalid'));
        return;
    }
    try {
        let preview: ViewDTO | ViewErrDTO;
        if (dimension.type === DimensionType.Raw) {
            preview = await getFactTableColumnPreview(dataset, factTable, dimension.factTableColumn);
        } else {
            preview = await getDimensionPreview(dataset, dimension, factTable, req.language);
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
    const dataset: Dataset = res.locals.dataset;
    const dimension: Dimension | undefined = dataset.dimensions.find(
        (dim: Dimension) => dim.id === req.params.dimension_id
    );
    if (!dimension) {
        next(new NotFoundException('errors.dimension_id_invalid'));
        return;
    }
    const factTable = getLatestRevision(dataset)?.factTables[0];
    if (!factTable) {
        next(new NotFoundException('errors.fact_table_invalid'));
        return;
    }
    let fileImport: FactTable;
    const utf8Buffer = convertBufferToUTF8(req.file.buffer);
    try {
        fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, res.locals.datasetId);
    } catch (err) {
        logger.error(`An error occurred trying to upload the file: ${err}`);
        next(new UnknownException('errors.upload_error'));
        return;
    }

    const tableMatcher = req.body as LookupTablePatchDTO;

    try {
        const result = await validateLookupTable(
            fileImport,
            factTable,
            dataset,
            dimension,
            utf8Buffer,
            tableMatcher
        );
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
    const dataset = res.locals.dataset;
    const dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);
    const factTable = getLatestRevision(dataset)?.factTables[0];
    if (!dimension) {
        next(new NotFoundException('errors.dimension_id_invalid'));
        return;
    }
    if (!factTable) {
        next(new NotFoundException('errors.fact_table_invalid'));
        return;
    }
    const dimensionPatchRequest = req.body as DimensionPatchDto;
    let preview: ViewDTO | ViewErrDTO;
    try {
        logger.debug(`User dimension type = ${JSON.stringify(dimensionPatchRequest)}`);
        switch (dimensionPatchRequest.dimension_type) {
            case DimensionType.TimePeriod:
            case DimensionType.TimePoint:
                logger.debug('Matching a Dimension containing Dates');
                preview = await validateDateTypeDimension(dimensionPatchRequest, dataset, dimension, factTable);
                break;
            case DimensionType.ReferenceData:
                logger.debug('Matching a Dimension containing Reference Data');
                preview = await validateReferenceData(
                    factTable,
                    dataset,
                    dimension,
                    dimensionPatchRequest.reference_type,
                    `${req.language}`
                );
                break;
            case DimensionType.LookupTable:
                logger.debug('User requested to patch a lookup table?');
                throw new Error('You need to post a lookup table with this request');
            default:
                throw new Error('Not Implemented Yet!');
        }
    } catch (error) {
        logger.error(`Something went wrong trying to validate the dimension with the following error: ${error}`);
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

export const updateDimensionInfo = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const dimension: Dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);
    const updatedInfo = req.body as DimensionInfoDTO;
    let info = dimension.dimensionInfo.find((info) => info.language === updatedInfo.language);
    if (!info) {
        info = new DimensionInfo();
        info.dimension = dimension;
        info.language = updatedInfo.language;
    }
    if (updatedInfo.name) {
        info.name = updatedInfo.name;
    }
    if (updatedInfo.notes) {
        info.notes = updatedInfo.notes;
    }
    await info.save();
    const updatedDimension = await Dimension.findOneByOrFail({ id: dimension.id });
    res.status(202);
    res.json(DimensionDTO.fromDimension(updatedDimension));
};
