import { Request, Response, NextFunction, Router } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { TopicRepository } from '../repositories/topic';
import { TopicDTO } from '../dtos/topic-dto';

export const topicRouter = Router();

topicRouter.get('/', async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('List topics');
    const topics = await TopicRepository.listAll();
    const topicDTOs = topics.map((topic) => TopicDTO.fromTopic(topic, req.language as Locale));
    res.json(topicDTOs);
  } catch (error) {
    logger.error(error, 'Error listing topics');
    next(new UnknownException());
  }
});
