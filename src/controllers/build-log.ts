import { Request, Response } from 'express';
import { BuildLog } from '../entities/dataset/build-log';
import { BuiltLogEntryDto } from '../dtos/build-log';
import { NotFoundException } from '../exceptions/not-found.exception';
import { logger } from '../utils/logger';

export const getBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getBuiltLogEntry = async (req: Request, res: Response): Promise<void> => {
  const buildId = req.params.build_id;
  if (!buildId) {
    throw new NotFoundException('Built not found.');
  }
  try {
    const build = await BuildLog.findOneByOrFail({ id: buildId });
    res.status(200).send(BuiltLogEntryDto.fromBuildLogFull(build));
  } catch (err) {
    logger.warn(err, `Failed to get build log entry with id ${buildId}`);
    throw new NotFoundException('Built not found.');
  }
};
