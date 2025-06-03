import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UserDTO } from '../dtos/user/user-dto';
import { User } from '../entities/user/user';
import { NotFoundException } from '../exceptions/not-found.exception';

export const userRouter = Router();

userRouter.get('/', async (req: Request, res: Response, next: NextFunction) => {
  logger.info('Getting the current user...');
  const user = req.user as User;

  if (!user) {
    next(new NotFoundException('errors.no_user'));
    return;
  }

  res.json(UserDTO.fromUser(user, req.language as Locale));
});
