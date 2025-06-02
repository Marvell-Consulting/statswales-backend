import { Router } from 'express';
import swaggerUi, { SwaggerUiOptions } from 'swagger-ui-express';

import consumerApiSpec from './openapi.json';

export const apiDocRouter = Router();

const opts: SwaggerUiOptions = {};

apiDocRouter.use('/', swaggerUi.serve);
apiDocRouter.get('/', swaggerUi.setup(consumerApiSpec, opts));
