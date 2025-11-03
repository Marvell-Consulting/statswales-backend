import { Request, Response } from 'express';
import { BuildLog } from '../entities/dataset/build-log';
import { BuiltLogEntryDto } from '../dtos/build-log';
import { NotFoundException } from '../exceptions/not-found.exception';
import { logger } from '../utils/logger';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';

export const getBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getFailedBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { status: CubeBuildStatus.Failed }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getCompletedBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { status: CubeBuildStatus.Completed }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getBaseCubeBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { type: CubeBuildType.BaseCube }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getValidationCubeBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { type: CubeBuildType.ValidationCube }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getFullCubeBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { type: CubeBuildType.FullCube }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getBulkDraftBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { type: CubeBuildType.DraftCubes }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
};

export const getBulkAllBuildLog = async (req: Request, res: Response): Promise<void> => {
  const pageSize = req.query.size ? Number.parseInt(req.query.size as string) : 30;
  const pageNo = req.query.page ? Number.parseInt(req.query.page as string) * pageSize : 0;
  const buildLog = await BuildLog.find({ where: { type: CubeBuildType.AllCubes }, take: pageSize, skip: pageNo });
  res.status(200).send(buildLog.map((log) => BuiltLogEntryDto.fromBuildLogLite(log)));
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
