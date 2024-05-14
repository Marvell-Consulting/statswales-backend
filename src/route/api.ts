import { Request, Response, Router } from 'express';
import multer from 'multer';
import pino from 'pino';

import { processCSV, uploadCSV, DEFAULT_PAGE_SIZE } from '../controllers/csv-processor';
import { DataLakeService } from '../controllers/datalake';
import { Datafile } from '../entity/Datafile';
import { FileDescription } from '../models/filelist';

const storage = multer.memoryStorage();
const upload = multer({ storage });

export const logger = pino({
    name: 'StatsWales-Alpha-App',
    level: 'debug'
});

export const apiRoute = Router();

apiRoute.get('/', (req, res) => {
    res.json({ message: req.t('api.available') });
});

apiRoute.post('/csv', upload.single('csv'), async (req: Request, res: Response) => {
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
    if (!req.body?.filename) {
        res.status(400);
        res.json({
            success: false,
            headers: undefined,
            data: undefined,
            errors: [
                {
                    field: 'filename',
                    message: 'No datasetname provided'
                }
            ]
        });
        return;
    }
    if (!req.body?.description) {
        res.status(400);
        res.json({
            success: false,
            headers: undefined,
            data: undefined,
            errors: [
                {
                    field: 'description',
                    message: 'No datasetname provided'
                }
            ]
        });
        return;
    }
    const datafile = new Datafile();
    datafile.name = req.body?.filename;
    datafile.description = req.body?.description;
    const saved_datafile_record = await datafile.save();
    const processedCSV = await uploadCSV(req.file?.buffer, saved_datafile_record);
    if (!processedCSV.success) {
        res.status(400);
    }
    res.json(processedCSV);
});

apiRoute.get('/csv/', async (req, res) => {
    const datafiles = await Datafile.find();
    const fileList: FileDescription[] = [];
    for (const datafile of datafiles) {
        fileList.push({
            name: datafile.name,
            id: datafile.id,
            description: datafile.description
        });
    }
    res.json({ filelist: fileList });
});

apiRoute.get('/csv/:file', async (req, res) => {
    const dataLakeService = new DataLakeService();
    const filename = req.params.file;
    const file = await dataLakeService.downloadFile(`${filename}.csv`);
    if (file === undefined || file === null) {
        res.status(404);
        res.json({ message: 'File not found... file is null or undefined' });
        return;
    }
    res.setHeader('Content-Length', file.length);
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=${filename}`);
    res.write(file, 'binary');
    res.end();
});

apiRoute.get('/csv/:file/view', async (req, res) => {
    const dataLakeService = new DataLakeService();
    const filename = req.params.file;
    const file = await dataLakeService.downloadFile(`${filename}.csv`);
    if (file === undefined || file === null) {
        res.status(404);
        res.json({ message: 'File not found... file is null or undefined' });
        return;
    }
    const page_number_str: string = req.query.page_number || req.body?.page_number;
    const page_size_str: string = req.query.page_size || req.body?.page_size;
    const page_number: number = Number.parseInt(page_number_str, 10) || 1;
    const page_size: number = Number.parseInt(page_size_str, 10) || DEFAULT_PAGE_SIZE;
    const processedCSV = await processCSV(filename, page_number, page_size);
    if (!processedCSV.success) {
        res.status(400);
    }
    res.json(processedCSV);
});
