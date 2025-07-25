import { Router } from 'express';
import swaggerUi, { SwaggerUiOptions } from 'swagger-ui-express';

import spec from './openapi.json';
import { appConfig } from '../../../config';

export const apiDocRouter = Router();

const opts: SwaggerUiOptions = {};
const config = appConfig();

// Replace the placeholder in the OpenAPI spec with the actual backend URL
const consumerApiSpec = JSON.parse(JSON.stringify(spec).replaceAll('{{backendURL}}', config.backend.url));

apiDocRouter.use('/', swaggerUi.serve);
apiDocRouter.get('/', swaggerUi.setup(consumerApiSpec, opts));
