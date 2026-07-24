import { NextFunction, Request, Response } from 'express';
import { BuildLog } from '../entities/dataset/build-log';
import { BuiltLogEntryDto } from '../dtos/build-log';
import { NotFoundException } from '../exceptions/not-found.exception';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { logger } from '../utils/logger';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';
import { GlobalRole } from '../enums/global-role';
import { BuildLogRepository } from '../repositories/build-log';
import { buildStatusValidator, buildTypeValidator, hasError } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { getUserGroupIdsForUser } from '../utils/get-permissions-for-user';

export const getBuildLog = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const globalRoles = req.user?.globalRoles ?? [];
  const isServiceAdminOrDeveloper =
    globalRoles.includes(GlobalRole.ServiceAdmin) || globalRoles.includes(GlobalRole.Developer);

  if (!isServiceAdminOrDeveloper) {
    logger.warn(`User ${req.user?.id} is not a service admin or developer, denying access to the build log`);
    next(new ForbiddenException('errors.build_log_not_permitted'));
    return;
  }

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

  let build: BuildLog;

  try {
    build = await BuildLog.findOneOrFail({
      where: { id: buildId },
      relations: { revision: { dataset: true } }
    });
  } catch (err) {
    logger.warn(err, `Failed to get build log entry with id ${buildId}`);
    throw new NotFoundException('Build not found.');
  }

  // when reached via the nested revision route, the build must belong to the revision
  // the caller has already been authorised for (res.locals.revision_id is set by loadRevision)
  if (res.locals.revision_id && build.revisionId !== res.locals.revision_id) {
    throw new NotFoundException('Build not found.');
  }

  const datasetUserGroupId = build.revision?.dataset?.userGroupId;
  const userGroupIds = getUserGroupIdsForUser(req.user!);
  const isDeveloper = req.user?.globalRoles.includes(GlobalRole.Developer);

  if (isDeveloper) {
    logger.warn(`User ${req.user?.id} is a developer, skipping group permissions check`);
  } else if (!datasetUserGroupId || !userGroupIds?.includes(datasetUserGroupId)) {
    logger.warn(`User does not have access to build ${buildId}`);
    throw new ForbiddenException('errors.dataset_not_in_users_groups');
  }

  res.status(200).send(BuiltLogEntryDto.fromBuildLogFull(build));
};
