import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource, DeepPartial, IsNull } from 'typeorm';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UserGroup } from '../entities/user/user-group';
import { Dataset } from '../entities/dataset/dataset';

// using hard coded uuids so that we can re-run the seeder for updates without creating new records
const stage1Group: DeepPartial<UserGroup> = {
  id: '24bf9f9c-898a-4d23-ae1e-35a6ff30ee63',
  metadata: [
    { name: 'Cam 1', email: 'cam1@example.com', language: Locale.WelshGb },
    { name: 'Stage 1', email: 'stage1@example.com', language: Locale.EnglishGb }
  ]
};

const defaultGroups: DeepPartial<UserGroup>[] = [stage1Group];

export default class UserGroupSeeder extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    await this.seedUserGroups(dataSource);
  }

  async seedUserGroups(dataSource: DataSource): Promise<UserGroup[]> {
    const savedGroups = await dataSource.getRepository(UserGroup).save(defaultGroups);

    logger.info('assigning any unassigned datasets to the stage 1 group');
    await dataSource.getRepository(Dataset).update({ userGroupId: IsNull() }, { userGroupId: stage1Group.id });

    return savedGroups;
  }
}
