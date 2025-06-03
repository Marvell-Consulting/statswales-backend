import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { UserDTO } from '../dtos/user/user-dto';
import { User } from '../entities/user/user';

export const userRouter = Router();

userRouter.get('/', async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('Getting the current user');
    const user = req.user as User;
    res.json(UserDTO.fromUser(user, req.language as Locale));
  } catch (error) {
    logger.error(error, 'Error listing topics');
    next(new UnknownException());
  }
});
