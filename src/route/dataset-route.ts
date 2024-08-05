/* eslint-disable import/no-cycle */
import { Request, Response, Router } from 'express';
import multer from 'multer';

import { ViewErrDTO } from '../dtos/view-dto';
import { ENGLISH, WELSH, t } from '../app';
import {
    processCSVFromDatalake,
    processCSVFromBlobStorage,
    uploadCSVToBlobStorage,
    DEFAULT_PAGE_SIZE
} from '../controllers/csv-processor';
import { DataLakeService } from '../controllers/datalake';
import { Dataset } from '../entity/dataset';
import { Datafile } from '../entity/datafile';
import { FileDescription } from '../models/filelist';
import { datasetToDatasetDTO } from '../dtos/dataset-dto';

const storage = multer.memoryStorage();
const upload = multer({ storage });
export const apiRoute = Router();

function isValidUUID(uuid: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuid.length === 36 && uuidRegex.test(uuid);
}

apiRoute.post('/', upload.single('csv'), async (req: Request, res: Response) => {
    if (!req.file) {
        const err: ViewErrDTO = {
            success: false,
            dataset_id: undefined,
            errors: [
                {
                    field: 'csv',
                    message: [
                        {
                            lang: ENGLISH,
                            message: t('errors.no_csv_data', { lng: ENGLISH })
                        },
                        {
                            lang: WELSH,
                            message: t('errors.no_csv_data', { lng: WELSH })
                        }
                    ],
                    tag: {
                        name: 'errors.no_csv_data',
                        params: {}
                    }
                }
            ]
        };
        res.status(400);
        res.json(err);
        return;
    }
    const lang: string = req.body?.language || req.i18n.language;
    const title: string = req.body?.title;
    if (!title) {
        const err: ViewErrDTO = {
            success: false,
            dataset_id: undefined,
            errors: [
                {
                    field: 'title',
                    message: [
                        {
                            lang: ENGLISH,
                            message: t('errors.no_title', { lng: ENGLISH })
                        },
                        {
                            lang: WELSH,
                            message: t('errors.no_title', { lng: WELSH })
                        }
                    ],
                    tag: {
                        name: 'errors.no_title',
                        params: {}
                    }
                }
            ]
        };
        res.status(400);
        res.json(err);
        return;
    }
    const dataset = Dataset.createDataset(title, 'BetaUser');
    const saved_dataset_record = await dataset.save();
    saved_dataset_record.addTitleByString(title, lang);
    const uploadDTO = await uploadCSVToBlobStorage(req.file?.buffer, saved_dataset_record);
    if (!uploadDTO.success) {
        res.status(400);
    }
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
    const datasetID = req.params.dataset;
    if (datasetID === undefined || datasetID === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... file is null or undefined' });
        return;
    }
    if (isValidUUID(datasetID) === false) {
        res.status(404);
        res.json({ message: 'Dataset not found...File ID is not Valid.' });
        return;
    }
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (!dataset) {
        res.status(404);
        res.json({ message: 'Dataset not found... dataset id is invalid' });
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
    if (datasetID === undefined || datasetID === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... You must specify a dataset ID' });
        return;
    }
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (dataset === undefined || dataset === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... Dataset ID not found in Database' });
        return;
    }
    if (isValidUUID(datasetID) === false) {
        res.status(404);
        res.json({ message: 'Dataset not found...File ID is not Valid.' });
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

apiRoute.get('/:dataset/xlsx', async (req, res) => {
    const datasetID = req.params.dataset;
    if (datasetID === undefined || datasetID === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... You must specify a dataset ID' });
        return;
    }
    if (isValidUUID(datasetID) === false) {
        res.status(404);
        res.json({ message: 'Dataset not found...File ID is not Valid.' });
        return;
    }
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (dataset === undefined || dataset === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... Dataset ID not found in Database' });
        return;
    }
    res.json({
        message: 'Not implmented yet'
    });
});

apiRoute.get('/:dataset/preview', async (req, res) => {
    const datasetID = req.params.dataset;
    if (datasetID === undefined || datasetID === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... You must specify a dataset ID' });
        return;
    }
    if (isValidUUID(datasetID) === false) {
        res.status(404);
        res.json({ message: 'Dataset not found...File ID is not Valid.' });
        return;
    }
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
        res.status(400);
    }
    res.json(processedCSV);
});

apiRoute.get('/:dataset/view', async (req, res) => {
    const datasetID = req.params.dataset;
    if (datasetID === undefined || datasetID === null) {
        res.status(404);
        res.json({ message: 'Dataset not found... You must specify a dataset ID' });
        return;
    }
    if (isValidUUID(datasetID) === false) {
        res.status(404);
        res.json({ message: 'Dataset not found...File ID is not Valid.' });
        return;
    }
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
        res.status(400);
    }
    res.json(processedCSV);
});
