import { NextFunction, Request, Response } from 'express';
import { BuildLog } from '../entities/dataset/build-log';
import { BuiltLogEntryDto } from '../dtos/build-log';
import { NotFoundException } from '../exceptions/not-found.exception';
import { logger } from '../utils/logger';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';
import { BuildLogRepository } from '../repositories/build-log';
import { buildStatusValidator, buildTypeValidator, hasError } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';

export const getBuildLog = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const typeError = req.query.type ? await hasError(buildTypeValidator(), req) : false;
  const statusError = req.query.status ? await hasError(buildStatusValidator(), req) : false;

  if (typeError) {
    const availableTypes = Object.values(CubeBuildType).join(', ');
    next(new BadRequestException(`type must be one of the following: ${availableTypes}`));
    return;
  }

  if (statusError) {
    const availableStatuses = Object.values(CubeBuildStatus).join(', ');
    next(new BadRequestException(`status must be one of the following: ${availableStatuses}`));
    return;
  }

  const buildType: CubeBuildType | undefined = req.query.type as CubeBuildType;
  const buildStatus: CubeBuildStatus | undefined = req.query.status as CubeBuildStatus;

  const buildLogs = await BuildLogRepository.getBy(buildType, buildStatus, pageSize, pageNo);
  res.status(200).send(buildLogs.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getBuiltLogEntry = async (req: Request, res: Response): Promise<void> => {
  const buildId = req.params.build_id;
  if (!buildId) {
    throw new NotFoundException('Build not found.');
  }
  try {
    const build = await BuildLog.findOneByOrFail({ id: buildId });
    res.status(200).send(BuiltLogEntryDto.fromBuildLogFull(build));
  } catch (err) {
    logger.warn(err, `Failed to get build log entry with id ${buildId}`);
    throw new NotFoundException('Build not found.');
  }
};
