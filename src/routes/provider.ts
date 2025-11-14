import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { ProviderRepository } from '../repositories/provider';
import { Locale } from '../enums/locale';
import { ProviderDTO } from '../dtos/provider-dto';
import { ProviderSourceDTO } from '../dtos/provider-source-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { NotFoundException } from '../exceptions/not-found.exception';
import { hasError, providerIdValidator } from '../validators';

const provider = Router();

provider.get('/', async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('List providers');
    const providers = await ProviderRepository.listAllByLanguage(req.language as Locale);
    const providerDTOs = providers.map((provider) => ProviderDTO.fromProvider(provider));
    res.json(providerDTOs);
  } catch (error) {
    logger.error(error, 'Error listing providers');
    next(new UnknownException());
  }
});

provider.get('/:provider_id/sources', async (req: Request, res: Response, next: NextFunction) => {
  const providerIdError = await hasError(providerIdValidator(), req);
  if (providerIdError) {
    next(new NotFoundException('errors.provider_id_invalid'));
    return;
  }

  try {
    logger.info('List provider sources');
    const providerSources = await ProviderRepository.listAllSourcesByProvider(
      req.params.provider_id,
      req.language as Locale
    );
    const providerSourceDTOs = providerSources.map((pSource) => ProviderSourceDTO.fromProviderSource(pSource));
    res.json(providerSourceDTOs);
  } catch (error) {
    logger.error(error, 'Error listing provider sources');
    next(new UnknownException());
  }
});

export const providerRouter = provider;
