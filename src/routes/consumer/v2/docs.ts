import { Router } from 'express';

import enSpec from './openapi-en.json';
// TODO: Re-enable Welsh docs once human translations are ready
// import cySpec from './openapi-cy.json';
import { config } from '../../../config';

export const apiV2DocRouter = Router();

// Replace the placeholder in the OpenAPI spec with the actual backend URL
const consumerApiSpecEn = JSON.parse(JSON.stringify(enSpec).replaceAll('{{backendURL}}', config.backend.url));
// const consumerApiSpecCy = JSON.parse(JSON.stringify(cySpec).replaceAll('{{backendURL}}', config.backend.url));

apiV2DocRouter.get('/swagger.json', (_req, res) => {
  res.json(consumerApiSpecEn);
});

// apiV2DocRouter.get('/swagger-cy.json', (_req, res) => {
//   res.json(consumerApiSpecCy);
// });

// swagger-ui-express uses a shared singleton for swagger-ui-init.js, so multiple swaggerUi.setup()
// calls in the same app overwrite each other. Redirect to the combined /docs/ UI instead,
// pre-selecting the v2 spec via the urls.primaryName query parameter.
apiV2DocRouter.get('/', (_req, res) => {
  res.redirect('/docs/?urls.primaryName=API%20v2%20(English)');
});
