import { Request, Response, Router } from 'express';
import multer from 'multer';
import pino from 'pino';

// eslint-disable-next-line import/no-cycle
import { processCSV, uploadCSV, DEFAULT_PAGE_SIZE } from '../controllers/csv-processor';
import { DataLakeService } from '../controllers/datalake';
import { Dataset } from '../entity/dataset';
import { Datafile } from '../entity/datafile';
import { FileDescription } from '../models/filelist';
import { datasetToDatasetDTO } from '../dtos/dataset-dto';

const storage = multer.memoryStorage();
const upload = multer({ storage });

export const logger = pino({
    name: 'StatsWales-Alpha-App',
    level: 'debug'
});

export const apiRoute = Router();

apiRoute.post('/', upload.single('csv'), async (req: Request, res: Response) => {
    if (!req.file) {
        res.status(400);
        res.json({
            success: false,
            headers: undefined,
            data: undefined,
            errors: [
                {
                    field: 'csv',
                    message: 'No CSV data available'
                }
            ]
        });
        return;
    }
    const internalName: string = req.body?.internal_name;
    if (!internalName) {
        res.status(400);
        res.json({
            success: false,
            errors: [
                {
                    field: 'internal_name',
                    message: 'No internal name for the dataset has been provided'
                }
            ]
        });
        return;
    }
    const dataset = Dataset.createDataset(internalName, 'BetaUser');
    const saved_dataset_record = await dataset.save();
    const uploadDTO = await uploadCSV(req.file?.buffer, saved_dataset_record);
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

apiRoute.get('/:dataset/view', async (req, res) => {
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
    const page_number_str: string = req.query.page_number || req.body?.page_number;
    const page_size_str: string = req.query.page_size || req.body?.page_size;
    const page_number: number = Number.parseInt(page_number_str, 10) || 1;
    const page_size: number = Number.parseInt(page_size_str, 10) || DEFAULT_PAGE_SIZE;
    const processedCSV = await processCSV(dataset, page_number, page_size);
    if (!processedCSV.success) {
        res.status(400);
    }
    res.json(processedCSV);
});
