import { publisherDataSource } from '../db/publisher-source';
import { BuildLog } from '../entities/dataset/build-log';
import { CubeBuildType } from '../enums/cube-build-type';
import { And, FindManyOptions, In, Not } from 'typeorm';
import { CubeBuildStatus } from '../enums/cube-build-status';

export const BuildLogRepository = publisherDataSource.getRepository(BuildLog).extend({
  async getBy(type?: CubeBuildType, status?: CubeBuildStatus, take = 30, skip = 0): Promise<BuildLog[]> {
    const findOpts: FindManyOptions<BuildLog> = {
      where: { status, type },
      take,
      skip
    };

    return BuildLog.find(findOpts);
  },

  async getByRevisionId(
    revisionId: string,
    type?: CubeBuildType,
    status?: CubeBuildStatus,
    take = 30,
    skip = 0
  ): Promise<BuildLog[]> {
    const findOpts: FindManyOptions<BuildLog> = {
      where: { revisionId, status, type },
      take,
      skip
    };

    return BuildLog.find(findOpts);
  },

  async getAllActiveBulkBuilds(): Promise<BuildLog[]> {
    return BuildLog.find({
      where: {
        type: In([CubeBuildType.AllCubes, CubeBuildType.DraftCubes, CubeBuildType.AllFilterTables]),
        status: And(Not(CubeBuildStatus.Completed), Not(CubeBuildStatus.Failed))
      }
    });
  },

  async getAllActiveBuilds(): Promise<BuildLog[]> {
    return BuildLog.find({
      where: { status: And(Not(CubeBuildStatus.Completed), Not(CubeBuildStatus.Failed)) }
    });
  }
});
