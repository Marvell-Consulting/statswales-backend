import fs from 'node:fs';
import path from 'node:path';

import swaggerAutogen from 'swagger-autogen';

import { schema } from '../src/routes/consumer/v1/schema';
import { schemaV2 } from '../src/routes/consumer/v2/schema';
import { translateSpec } from '../src/routes/consumer/translate-openapi';
import { v1CyTranslations } from '../src/routes/consumer/v1/translations-cy';
import { v2CyTranslations } from '../src/routes/consumer/v2/translations-cy';

/**
 * This script generates the OpenAPI spec files for the StatsWales 3 Consumer API.
 *
 * For each API version it generates:
 *   - openapi-en.json  (English, from swagger-autogen)
 *   - openapi-cy.json  (Welsh, by post-processing the English spec with a translation map)
 *
 * This should run automatically on build, but can also be run manually with `npm run docs:generate`.
 */
async function main(): Promise<void> {
  const generateDocs = swaggerAutogen({ openapi: '3.1.1', language: 'en-GB' });

  // Generate v1 English spec
  const consumerEndpointsV1 = ['./src/routes/consumer/v1/api.ts'];
  const outputFileV1 = path.join(__dirname, '../src/routes/consumer/v1/openapi-en.json');
  await generateDocs(outputFileV1, consumerEndpointsV1, schema);

  // Generate v1 Welsh spec
  const v1EnSpec = JSON.parse(fs.readFileSync(outputFileV1, 'utf-8'));
  const v1CySpec = translateSpec(v1EnSpec, v1CyTranslations);
  fs.writeFileSync(
    path.join(__dirname, '../src/routes/consumer/v1/openapi-cy.json'),
    JSON.stringify(v1CySpec, null, 2)
  );

  // Generate v2 English spec
  const consumerEndpointsV2 = ['./src/routes/consumer/v2/api.ts'];
  const outputFileV2 = path.join(__dirname, '../src/routes/consumer/v2/openapi-en.json');
  await generateDocs(outputFileV2, consumerEndpointsV2, schemaV2);

  // Generate v2 Welsh spec
  const v2EnSpec = JSON.parse(fs.readFileSync(outputFileV2, 'utf-8'));
  const v2CySpec = translateSpec(v2EnSpec, v2CyTranslations);
  fs.writeFileSync(
    path.join(__dirname, '../src/routes/consumer/v2/openapi-cy.json'),
    JSON.stringify(v2CySpec, null, 2)
  );
}

main().catch((err) => {
  process.stderr.write(`${err}\n`);
  process.exitCode = 1;
});
