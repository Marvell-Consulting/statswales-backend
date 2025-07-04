import path from 'node:path';

import swaggerAutogen from 'swagger-autogen';

import { schema } from '../src/routes/consumer/v1/schema';

/**
 * This script generates the OpenAPI spec file for the StatsWales 3 Consumer API and saves it at
 * src/routes/consumer/v1/openapi.json, which is then used to render the documentation via
 * src/routes/consumer/v1/docs.ts.
 *
 * This should run automatically on build, but can also be run manually with `npm run docs:generate`.
 */
const consumerEndpoints = ['./src/routes/consumer/v1/api.ts'];
const outputFile = path.join(__dirname, '../src/routes/consumer/v1/openapi.json');

const generateDocs = swaggerAutogen({ openapi: '3.1.1', language: 'en-GB' });
generateDocs(outputFile, consumerEndpoints, schema);
