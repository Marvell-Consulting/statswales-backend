import { Router } from 'express';
import swaggerUi, { SwaggerUiOptions } from 'swagger-ui-express';

import spec from './openapi.json';
import { config } from '../../../config';

export const apiV2DocRouter = Router();

const opts: SwaggerUiOptions = { customSiteTitle: 'StatsWales API v2' };

// Replace the placeholder in the OpenAPI spec with the actual backend URL
const consumerApiSpec = JSON.parse(JSON.stringify(spec).replaceAll('{{backendURL}}', config.backend.url));

apiV2DocRouter.get('/swagger.json', (_req, res) => {
  res.json(consumerApiSpec);
});
apiV2DocRouter.use('/', swaggerUi.serve);
apiV2DocRouter.get('/', swaggerUi.setup(consumerApiSpec, opts));
