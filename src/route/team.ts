import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { TeamRepository } from '../repositories/team';
import { TeamDTO } from '../dtos/team-dto';
import { hasError, teamIdValidator } from '../validators';
import { NotFoundException } from '../exceptions/not-found.exception';

export const teamRouter = Router();

teamRouter.get('/', async (req: Request, res: Response, next: NextFunction) => {
    try {
        logger.info('List teams');
        const teams = await TeamRepository.listAll(req.language as Locale);
        const teamDTOs = teams.map((team) => TeamDTO.fromTeam(team, req.language as Locale));
        res.json(teamDTOs);
    } catch (error) {
        logger.error('Error listing teams', error);
        next(new UnknownException());
    }
});

teamRouter.get('/:team_id', async (req: Request, res: Response, next: NextFunction) => {
    const teamIdError = await hasError(teamIdValidator(), req);
    if (teamIdError) {
        logger.error(teamIdError);
        next(new NotFoundException('errors.team_id_invalid'));
        return;
    }

    try {
        logger.debug(`Loading team ${req.params.team_id}...`);
        const team = await TeamRepository.getById(req.params.team_id);
        res.json(TeamDTO.fromTeam(team, req.language as Locale));
    } catch (err) {
        logger.error(`Failed to load team, error: ${err}`);
        next(new NotFoundException('errors.no_team'));
        return;
    }

    next();
});
