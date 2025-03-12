import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { OrganisationRepository } from '../repositories/organisation';
import { OrganisationDTO } from '../dtos/organisation-dto';

export const organisationRouter = Router();

organisationRouter.get('/', async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('List organisations');
    const organisations = await OrganisationRepository.listAll();
    const organisationDTOs = organisations.map((org) => OrganisationDTO.fromOrganisation(org, req.language as Locale));
    res.json(organisationDTOs);
  } catch (error) {
    logger.error('Error listing organisations', error);
    next(new UnknownException());
  }
});
