import { publisherDataSource } from '../db/publisher-source';
import { BuildLog } from '../entities/dataset/build-log';
import { CubeBuildType } from '../enums/cube-build-type';
import { And, FindManyOptions, In, LessThan, Not } from 'typeorm';
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
  },

  // builds still active (not completed/failed) long after they started have almost certainly been
  // abandoned by a process crash or restart, rather than genuinely still running
  async getStuckBuilds(startedBefore: Date): Promise<BuildLog[]> {
    return BuildLog.find({
      where: {
        status: And(Not(CubeBuildStatus.Completed), Not(CubeBuildStatus.Failed)),
        startedAt: LessThan(startedBefore)
      }
    });
  }
});
