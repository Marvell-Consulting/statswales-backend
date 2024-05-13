import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import app, { connectToDb } from '../src/app';
import { Datafile } from '../src/entity/Datafile';

import { datasourceOptions } from './test-data-source';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.uploadFile = jest.fn();

beforeAll(async () => {
    await connectToDb(datasourceOptions);
    const datafile1 = new Datafile();
    datafile1.name = 'test-data-1.csv';
    datafile1.description = 'Test Data File 1';
    datafile1.id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
    await datafile1.save();
    const datafile2 = new Datafile();
    datafile2.name = 'test-data-2.csv';
    datafile2.description = 'Test Data File 2';
    datafile2.id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
    await datafile2.save();
    console.log(`Datafile created: ${Datafile.find()}`);
});

describe('API Endpoints', () => {
    test('Inital API endpoint works', async () => {
        const res = await request(app).get('/en-GB/api/');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ message: 'api.available' });
    });

    test('Upload returns 400 if no file attached', async () => {
        const res = await request(app).post('/en-GB/api/csv').query({ filename: 'test-data-1.csv' });
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

    test('Upload returns 400 if no filename is given', async () => {
        const csvfile = path.resolve(__dirname, `./test-data-1.csv`);
        const res = await request(app).post('/en-GB/api/csv').attach('csv', csvfile);
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            errors: [
                {
                    field: 'filename',
                    message: 'No datasetname provided'
                }
            ]
        });
    });

    test('Upload returns 200 if a file is attached', async () => {
        const csvfile = path.resolve(__dirname, `./test-data-1.csv`);

        const res = await request(app)
            .post('/en-GB/api/csv')
            .attach('csv', csvfile)
            .field('filename', 'test-upload-data-1')
            .field('description', 'Test Data File 1');
        const datafile: Datafile | null = await Datafile.findOneBy({ name: 'test-upload-data-1' });
        expect(res.status).toBe(200);
        expect(res.body).toEqual({
            success: true,
            datafile_id: datafile?.id,
            datafile_name: datafile?.name,
            datafile_description: datafile?.description
        });
        datafile?.remove();
    });

    test('Get a filelist list returns 200 with a file list', async () => {
        const res = await request(app).get('/en-GB/api/csv');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({
            filelist: [
                {
                    name: 'test-data-1.csv',
                    id: 'bdc40218-af89-424b-b86e-d21710bc92f1',
                    description: 'Test Data File 1'
                },
                {
                    name: 'test-data-2.csv',
                    id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359',
                    description: 'Test Data File 2'
                }
            ]
        });
    });

    test('Get file view returns 400 if page_number is too high', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer);
        const res = await request(app).get('/en-GB/api/csv/test-data-2.csv/view').query({ page_number: 20 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            datafile_id: 'test-data-2.csv',
            datafile_name: undefined,
            datafile_description: undefined,
            page_size: undefined,
            current_page: undefined,
            total_pages: undefined,
            headers: undefined,
            data: undefined,
            errors: [
                {
                    field: 'page_number',
                    message: 'Page number must be less than or equal to 6'
                }
            ]
        });
    });

    test('Get file view returns 400 if page_size is too high', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer);

        const res = await request(app).get('/en-GB/api/csv/test-data-2.csv/view').query({ page_size: 1000 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            datafile_id: 'test-data-2.csv',
            datafile_name: undefined,
            datafile_description: undefined,
            page_size: undefined,
            current_page: undefined,
            total_pages: undefined,
            headers: undefined,
            data: undefined,
            errors: [
                {
                    field: 'page_size',
                    message: 'Page size must be between 5 and 500'
                }
            ]
        });
    });

    test('Get file view returns 400 if page_size is too low', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer);

        const res = await request(app).get('/en-GB/api/csv/test-data-2.csv/view').query({ page_size: 1 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            datafile_id: 'test-data-2.csv',
            datafile_name: undefined,
            datafile_description: undefined,
            page_size: undefined,
            current_page: undefined,
            total_pages: undefined,
            headers: undefined,
            data: undefined,
            errors: [
                {
                    field: 'page_size',
                    message: 'Page size must be between 5 and 500'
                }
            ]
        });
    });

    test('Get file rertunrs 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer.toString());

        const res = await request(app).get('/en-GB/api/csv/test-data-2.csv');
        expect(res.status).toBe(200);
        expect(res.text).toEqual(testFile2Buffer.toString());
    });

    test('Get file view returns 200 and correct page data', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile1Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile1Buffer.toString());

        const res = await request(app)
            .get('/en-GB/api/csv/fa07be9d-3495-432d-8c1f-d0fc6daae359/view')
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(200);
        expect(res.body.current_page).toBe(2);
        expect(res.body.total_pages).toBe(6);
        expect(res.body.page_size).toBe(100);
        expect(res.body.headers).toEqual(['ID', 'Text', 'Number', 'Date']);
        expect(res.body.data[0]).toEqual(['101', 'GEYiRzLIFM', '774477', '2002-03-13']);
        expect(res.body.data[99]).toEqual(['200', 'QhBxdmrUPb', '3256099', '2026-12-17']);
    });

    test('Get file view returns 404 when a non-existant file is requested', async () => {
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(null);

        const res = await request(app).get('/en-GB/api/csv/test-data-4.csv');
        expect(res.status).toBe(404);
        expect(res.body).toEqual({ message: 'File not found... file is null or undefined' });
    });

    test('Get file view returns 404 when a non-existant file view is requested', async () => {
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(null);

        const res = await request(app).get('/en-GB/api/csv/test-data-4.csv/view');
        expect(res.status).toBe(404);
        expect(res.body).toEqual({ message: 'File not found... file is null or undefined' });
    });
});
