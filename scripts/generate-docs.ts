import path from 'node:path';

import swaggerAutogen from 'swagger-autogen';

import { schema } from '../src/routes/consumer/v1/schema';
import { schemaV2 } from '../src/routes/consumer/v2/schema';

/**
 * This script generates the OpenAPI spec file for the StatsWales 3 Consumer API and saves it at
 * src/routes/consumer/v1/openapi.json, which is then used to render the documentation via
 * src/routes/consumer/v1/docs.ts.
 *
 * This should run automatically on build, but can also be run manually with `npm run docs:generate`.
 */
const generateDocs = swaggerAutogen({ openapi: '3.1.1', language: 'en-GB' });

const consumerEndpointsV1 = ['./src/routes/consumer/v1/api.ts'];
const outputFileV1 = path.join(__dirname, '../src/routes/consumer/v1/openapi.json');
generateDocs(outputFileV1, consumerEndpointsV1, schema);

const consumerEndpointsV2 = ['./src/routes/consumer/v2/api.ts'];
const outputFileV2 = path.join(__dirname, '../src/routes/consumer/v2/openapi.json');
generateDocs(outputFileV2, consumerEndpointsV2, schemaV2);
