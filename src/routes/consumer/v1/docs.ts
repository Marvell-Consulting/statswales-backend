import { Router } from 'express';

import enSpec from './openapi-en.json';
// TODO: Re-enable Welsh docs once human translations are ready
// import cySpec from './openapi-cy.json';
import { config } from '../../../config';

export const apiDocRouter = Router();

// Replace the placeholder in the OpenAPI spec with the actual backend URL
const consumerApiSpecEn = JSON.parse(JSON.stringify(enSpec).replaceAll('{{backendURL}}', config.backend.url));
// const consumerApiSpecCy = JSON.parse(JSON.stringify(cySpec).replaceAll('{{backendURL}}', config.backend.url));

apiDocRouter.get('/swagger.json', (_req, res) => {
  res.json(consumerApiSpecEn);
});

// apiDocRouter.get('/swagger-cy.json', (_req, res) => {
//   res.json(consumerApiSpecCy);
// });

// swagger-ui-express uses a shared singleton for swagger-ui-init.js, so multiple swaggerUi.setup()
// calls in the same app overwrite each other. Redirect to the combined /docs/ UI instead,
// pre-selecting the v1 spec via the urls.primaryName query parameter.
apiDocRouter.get('/', (_req, res) => {
  res.redirect('/docs/?urls.primaryName=API%20v1%20(English)');
});
