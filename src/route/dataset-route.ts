/* eslint-disable no-warning-comments */
/* eslint-disable import/no-cycle */
import { Request, Response, Router } from 'express';
import multer from 'multer';
import pino from 'pino';

import { ViewErrDTO } from '../dtos/view-dto';
import { ENGLISH, WELSH, t } from '../app';
import {
    processCSVFromDatalake,
    processCSVFromBlobStorage,
    uploadCSVToBlobStorage,
    DEFAULT_PAGE_SIZE
} from '../controllers/csv-processor';
import { DataLakeService } from '../controllers/datalake';
import { Dataset } from '../entity2/dataset';
import { DatasetInfo } from '../entity2/dataset_info';
import { DatasetRevision } from '../entity2/revision';
import { Import } from '../entity2/import';
import { FileDescription } from '../models/filelist';
import { datasetToDatasetDTO } from '../dtos/dataset-dto';

export const logger = pino({
    name: 'StatsWales-Alpha-App: DatasetRoute',
    level: 'debug'
});

const storage = multer.memoryStorage();
const upload = multer({ storage });
export const apiRoute = Router();

function isValidUUID(uuid: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuid.length === 36 && uuidRegex.test(uuid);
}

function checkDatasetID(datasetID: string, res: Response): boolean {
    if (datasetID === undefined || datasetID === null) {
        res.status(400);
        res.json({ message: 'Dataset ID is null or undefined' });
        return false;
    }
    if (isValidUUID(datasetID) === false) {
        res.status(400);
        res.json({ message: 'Dataset ID is not valid' });
        return false;
    }
    return true;
}

function errorDtoGenerator(
    field: string,
    translationString: string,
    datasetID: string | undefined = undefined
): ViewErrDTO {
    return {
        success: false,
        dataset_id: datasetID,
        errors: [
            {
                field,
                message: [
                    {
                        lang: ENGLISH,
                        message: t(translationString, { lng: ENGLISH })
                    },
                    {
                        lang: WELSH,
                        message: t(translationString, { lng: WELSH })
                    }
                ],
                tag: {
                    name: translationString,
                    params: {}
                }
            }
        ]
    };
}


apiRoute.post('/', upload.single('csv'), async (req: Request, res: Response) => {
    if (!req.file) {
        res.status(400);
        res.json(errorDtoGenerator('csv', 'errors.no_csv_data'));
        return;
    }
    const lang: string = req.body?.language || req.i18n.language;
    const title: string = req.body?.title;
    if (!title) {
        res.status(400);
        res.json(errorDtoGenerator('title', 'errors.no_title'));
        return;
    }
    let importRecord: Import;
    try {
        importRecord = await uploadCSVToBlobStorage(req.file?.stream, req.file?.mimetype);
    } catch (err) {
        logger.error(`An error occured trying to upload the file with the following error: ${e}`);
        res.status(500);
        res.json({ message: 'Error uploading file' });
        return;
    }

    // Everything looks good so far, let's create the dataset and revision records
    const dataset = new Dataset();
    dataset.creation_date = new Date();
    // TODO change how we handle authentication to get the user on the Backend
    dataset.created_by = 'Test User';
    const saved_dataset_record = await dataset.save();
    const datasetInfo = new DatasetInfo();
    datasetInfo.language = lang;
    datasetInfo.title = title;
    datasetInfo.dataset = saved_dataset_record;
    datasetInfo.save();
    const revision = new DatasetRevision();
    revision.dataset = saved_dataset_record;
    revision.revision_index = 1;
    revision.creation_date = new Date();
    // TODO change how we handle authentication to get the user on the Backend
    revision.created_by = 'Test User';
    const saved_revision_record = await revision.save();
    importRecord.revision = saved_revision_record;
    importRecord.save();

    res.json(uploadDTO);
});

apiRoute.get('/', async (req, res) => {
    const datasets = await Dataset.find();
    const fileList: FileDescription[] = [];
    for (const dataset of datasets) {
        fileList.push({
            internal_name: dataset.internalName,
            id: dataset.id
        });
    }
    res.json({ filelist: fileList });
});

apiRoute.get('/:dataset', async (req, res) => {
    const datasetID: string = req.params.dataset;
    if (!checkDatasetID(datasetID, res)) return;
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (!dataset) {
        res.status(404);
        res.json({ message: 'Dataset not found.' });
        return;
    }
    const datafiles = await dataset.datafiles;
    if (datafiles.length < 1) {
        res.status(404);
        res.json({ message: 'Dataset has no datafiles attached' });
        return;
    }
    const dto = await datasetToDatasetDTO(dataset);
    res.json(dto);
});

apiRoute.get('/:dataset/csv', async (req, res) => {
    const dataLakeService = new DataLakeService();
    const datasetID = req.params.dataset;
    if (!checkDatasetID(datasetID, res)) return;
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (dataset === undefined || dataset === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... Dataset ID not found in Database' });
        return;
    }
    const datafiles = await dataset.datafiles;
    const fileToDownload: Datafile | undefined = datafiles
        .sort(
            (first: Datafile, second: Datafile) =>
                new Date(second.creationDate).getTime() - new Date(first.creationDate).getTime()
        )
        .shift();
    if (fileToDownload === undefined || fileToDownload === null) {
        res.status(404);
        res.json({ message: 'Dataset has no file attached' });
        return;
    }
    const file = await dataLakeService.downloadFile(`${fileToDownload.id}.csv`);
    if (file === undefined || file === null) {
        res.status(404);
        res.json({ message: 'File not found... file is null or undefined' });
        return;
    }
    res.setHeader('Content-Length', file.length);
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=${fileToDownload.id}.csv`);
    res.write(file, 'binary');
    res.end();
});

apiRoute.get('/:dataset/preview', async (req, res) => {
    const datasetID = req.params.dataset;
    if (!checkDatasetID(datasetID, res)) return;
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (dataset === undefined || dataset === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... Dataset ID not found in Database' });
        return;
    }
    const page_number_str: string = req.query.page_number || req.body?.page_number;
    const page_size_str: string = req.query.page_size || req.body?.page_size;
    const page_number: number = Number.parseInt(page_number_str, 10) || 1;
    const page_size: number = Number.parseInt(page_size_str, 10) || DEFAULT_PAGE_SIZE;
    const processedCSV = await processCSVFromBlobStorage(dataset, page_number, page_size);
    if (!processedCSV.success) {
        res.status(500);
    }
    res.json(processedCSV);
});

apiRoute.get('/:dataset/view', async (req, res) => {
    const datasetID = req.params.dataset;
    if (!checkDatasetID(datasetID, res)) return;
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (dataset === undefined || dataset === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... Dataset ID not found in Database' });
        return;
    }
    const page_number_str: string = req.query.page_number || req.body?.page_number;
    const page_size_str: string = req.query.page_size || req.body?.page_size;
    const page_number: number = Number.parseInt(page_number_str, 10) || 1;
    const page_size: number = Number.parseInt(page_size_str, 10) || DEFAULT_PAGE_SIZE;
    const processedCSV = await processCSVFromDatalake(dataset, page_number, page_size);
    if (!processedCSV.success) {
        res.status(500);
    }
    res.json(processedCSV);
});
