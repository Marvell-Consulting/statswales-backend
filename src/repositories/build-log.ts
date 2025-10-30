import { dataSource } from '../db/data-source';
import { BuildLog } from '../entities/dataset/build-log';
import { CubeBuildType } from '../enums/cube-build-type';
import { In, Not } from 'typeorm';
import { CubeBuildStatus } from '../enums/cube-build-status';

export const BuildLogRepository = dataSource.getRepository(BuildLog).extend({
  async getAllActiveBulkBuilds(): Promise<BuildLog[]> {
    return BuildLog.find({
      where: { type: In([CubeBuildType.AllCubes, CubeBuildType.DraftCubes]), status: Not(CubeBuildStatus.Completed) }
    });
  },

  async getAllActiveBuilds(): Promise<BuildLog[]> {
    return BuildLog.find({
      where: { status: Not(CubeBuildStatus.Completed) }
    });
  }
});
