import { Router } from 'express';
import swaggerUi, { SwaggerUiOptions } from 'swagger-ui-express';

export const combinedDocRouter = Router();

const opts: SwaggerUiOptions = {
  explorer: true,
  swaggerOptions: {
    urls: [
      { url: '/v2/docs/swagger.json', name: 'API v2 (English)' },
      { url: '/v2/docs/swagger-cy.json', name: 'API v2 (Cymraeg)' },
      { url: '/v1/docs/swagger.json', name: 'API v1 (English)' },
      { url: '/v1/docs/swagger-cy.json', name: 'API v1 (Cymraeg)' }
    ],
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'urls.primaryName': 'API v2 (English)'
  }
};

combinedDocRouter.use('/', swaggerUi.serve);
combinedDocRouter.get('/', swaggerUi.setup(null, { ...opts, customSiteTitle: 'StatsWales API' }));
