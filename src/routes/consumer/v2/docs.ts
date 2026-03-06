import { Router } from 'express';

import spec from './openapi.json';
import { config } from '../../../config';

export const apiV2DocRouter = Router();

// Replace the placeholder in the OpenAPI spec with the actual backend URL
const consumerApiSpec = JSON.parse(JSON.stringify(spec).replaceAll('{{backendURL}}', config.backend.url));

apiV2DocRouter.get('/swagger.json', (_req, res) => {
  res.json(consumerApiSpec);
});

// swagger-ui-express uses a shared singleton for swagger-ui-init.js, so multiple swaggerUi.setup()
// calls in the same app overwrite each other. Redirect to the combined /docs/ UI instead,
// pre-selecting the v2 spec via the urls.primaryName query parameter.
apiV2DocRouter.get('/', (_req, res) => {
  res.redirect('/docs/?urls.primaryName=API%20v2%20(current)');
});
