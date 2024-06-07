import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import app, { dbManager, connectToDb } from '../src/app';
import { Dataset } from '../src/entity/dataset';
import { Datafile } from '../src/entity/datafile';
import { datasetToDatasetDTO } from '../src/dtos/dataset-dto';

import { datasourceOptions } from './test-data-source';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.uploadFile = jest.fn();

describe('API Endpoints', () => {
    beforeAll(async () => {
        console.log('Creating Database');
        await connectToDb(datasourceOptions);
        await dbManager.initializeDataSource();

        const dataset1 = Dataset.createDataset('Test Data 1', 'test', 'bdc40218-af89-424b-b86e-d21710bc92f1');
        dataset1.live = true;
        dataset1.code = 'tst0001';
        await dataset1.save();
        dataset1.addTitleByString('Test Dataset 1', 'EN');
        dataset1.addDescriptionByString('I am the first test dataset', 'EN');

        const dataset2 = Dataset.createDataset('Test Data 2', 'test', 'fa07be9d-3495-432d-8c1f-d0fc6daae359');
        dataset2.live = true;
        dataset2.code = 'tst0002';
        dataset2.createdBy = 'test';
        await dataset2.save();
        const datafile2 = new Datafile();
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        datafile2.sha256hash = createHash('sha256').update(testFile2Buffer).digest('hex');
        datafile2.createdBy = 'test';
        await dataset2.addDatafile(datafile2);
        dataset2.addTitleByString('Test Dataset 2', 'EN');
        dataset2.addDescriptionByString('I am the second test dataset', 'EN');

        const datafilefromdb = await Datafile.findOneBy({ sha256hash: datafile2.sha256hash });
        if (datafilefromdb) console.log(`Datafile2 from DB = ${JSON.stringify(datafilefromdb)}`);

        const datasetfromdb = await Dataset.findOneBy({ id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359' });
        if (datasetfromdb) {
            const dto = await datasetToDatasetDTO(datasetfromdb);
            console.log(`Dataset 2 from DB = ${JSON.stringify(dto)}`);
        } else console.log(`datafile 2 not found`);
    });

    test('Upload returns 400 if no file attached', async () => {
        const res = await request(app).post('/en-GB/dataset').query({ filename: 'test-data-1.csv' });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            errors: [
                {
                    field: 'csv',
                    message: 'No CSV data available'
                }
            ]
        });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
    });
});
