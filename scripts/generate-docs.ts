/* eslint-disable no-console */
import 'dotenv/config';
import path from 'node:path';

import swaggerAutogen from 'swagger-autogen';

/**
 * This script generates the OpenAPI spec file for the StatsWales 3 Consumer API, which is then used to render the api
 * documentation via src/routes/api-doc.ts.
 *
 * This should run automatically on build, but can also be run manually with `npm run docs:generate`.
 */
const consumerEndpoints = ['./src/routes/consumer/v1/api.ts'];
const outputFile = path.join(__dirname, '../src/routes/consumer/v1/openapi.json');

const doc = {
  info: {
    version: '1.0.0',
    title: 'StatsWales 3 Consumer API',
    description: 'This website provides documentation for StatsWales 3 public API.',
    contact: {
      name: 'StatsWales Support',
      email: 'StatsWales@gov.wales'
    }
  },
  servers: [
    { description: 'Local', url: 'https://localhost:3001/v1' },
    { description: 'Production', url: 'https://api.stats.gov.wales/v1' }
  ],
  components: {
    parameters: {
      language: {
        name: 'accept-language',
        in: 'header',
        description: 'Language for the response. Supported languages: "cy" for Welsh, "en" for English',
        required: false,
        type: 'string',
        default: 'en'
      },
      page: {
        name: 'page',
        in: 'query',
        description: 'Page number for pagination',
        required: false,
        type: 'integer',
        default: 1
      },
      limit: {
        name: 'limit',
        in: 'query',
        description: 'Number of datasets per page',
        required: false,
        type: 'integer',
        default: 10
      }
    }
  }
};

const generateDocs = swaggerAutogen({ openapi: '3.1.1', language: 'en-GB' });

generateDocs(outputFile, consumerEndpoints, doc);
