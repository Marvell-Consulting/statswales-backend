import { NextFunction, Request, Response } from 'express';

import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTable } from '../entities/dataset/fact-table';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataLakeService } from '../services/datalake';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';
import { getMeasurePreview, validateMeasureLookupTable } from '../services/measure-handler';
import { uploadCSV } from '../services/csv-processor';
import { convertBufferToUTF8 } from '../utils/file-utils';

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
        const datalakeService = new DataLakeService();
        await datalakeService.deleteFile(measureLookupFilename, dataset.id);
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
    const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
    res.status(200);
    const dto = DatasetDTO.fromDataset(updateDataset);
    res.json(dto);
};

export const attachLookupTableToMeasure = async (req: Request, res: Response, next: NextFunction) => {
    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }
    const dataset: Dataset = res.locals.dataset;

    // Replace calls that require this to calls that get a single factTable for all revisions to "present"
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

    const tableMatcher = req.body as MeasureLookupPatchDTO;

    try {
        const result = await validateMeasureLookupTable(fileImport, factTable, dataset, req.file.buffer, tableMatcher);
        if ((result as ViewErrDTO).status) {
            const error = result as ViewErrDTO;
            res.status(error.status);
            res.json(result);
            return;
        }
        res.status(200);
        res.json(result);
    } catch (err) {
        logger.error(`An error occurred trying to handle measure lookup table with error: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }
};

export const getPreviewOfMeasure = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const factTable = getLatestRevision(dataset)?.factTables[0];
    if (!dataset.measure) {
        next(new NotFoundException('errors.measure_invalid'));
        return;
    }
    if (!factTable) {
        next(new NotFoundException('errors.fact_table_invalid'));
        return;
    }
    try {
        const preview = await getMeasurePreview(dataset, factTable);
        res.status(200);
        res.json(preview);
    } catch (err) {
        logger.error(`Something went wrong trying to get a preview of the dimension with the following error: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }
};
