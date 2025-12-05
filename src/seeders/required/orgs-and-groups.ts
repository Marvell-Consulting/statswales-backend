import 'dotenv/config';
import { DataSource, DeepPartial, IsNull } from 'typeorm';

import { logger } from '../../utils/logger';
import { dataSource } from '../../db/data-source';
import { Organisation } from '../../entities/user/organisation';
import { Locale } from '../../enums/locale';
import { Dataset } from '../../entities/dataset/dataset';
import { UserGroup } from '../../entities/user/user-group';

// using hard coded uuids so that we can re-run the seeder for updates without creating new records
const organisations: DeepPartial<Organisation>[] = [
  {
    id: '4ef4facf-c488-4837-a65b-e66d4b525965',
    metadata: [
      { name: 'Welsh Government', language: Locale.EnglishGb },
      { name: 'Llywodraeth Cymru', language: Locale.WelshGb }
    ]
  },
  {
    id: '51326112-33c1-4bdf-b51f-76f630ef4c48',
    metadata: [
      { name: 'Welsh Revenue Authority', language: Locale.EnglishGb },
      { name: 'Awdurdod Cyllid Cymru', language: Locale.WelshGb }
    ]
  },
  {
    id: 'a0fec332-9ca3-42bf-bdf7-98e7a222cb6a',
    metadata: [
      { name: 'Medr', language: Locale.EnglishGb },
      { name: 'Medr', language: Locale.WelshGb }
    ]
  }
];

// using hard coded uuids so that we can re-run the seeder for updates without creating new records
export const stage1Group: DeepPartial<UserGroup> = {
  id: '24bf9f9c-898a-4d23-ae1e-35a6ff30ee63',
  organisationId: '4ef4facf-c488-4837-a65b-e66d4b525965', // Welsh Government
  metadata: [
    { name: 'Cam 1', email: 'cam1@example.com', language: Locale.WelshGb },
    { name: 'Stage 1', email: 'stage1@example.com', language: Locale.EnglishGb }
  ]
};

export class OrgsAndGroupsSeeder {
  constructor(private ds: DataSource) {
    this.ds = ds;
  }

  async run(): Promise<void> {
    logger.info('Starting OrgsAndGroupsSeeder...');
    await this.seedOrganisations();
    await this.seedUserGroups();
    logger.info('OrgsAndGroupsSeeder finished.');
  }

  async seedOrganisations(): Promise<Organisation[]> {
    const savedOrgs = await this.ds.getRepository(Organisation).save(organisations);
    logger.info(`Seeded ${savedOrgs.length} organisations`);
    return savedOrgs;
  }

  async seedUserGroups(): Promise<UserGroup[]> {
    const defaultGroups: DeepPartial<UserGroup>[] = [stage1Group];
    const savedGroups = await this.ds.getRepository(UserGroup).save(defaultGroups);
    logger.info(`Seeded ${savedGroups.length} groups`);

    logger.info('assigning any unassigned datasets to the stage 1 group');
    await this.ds.getRepository(Dataset).update({ userGroupId: IsNull() }, { userGroupId: stage1Group.id });
    return savedGroups;
  }
}

Promise.resolve()
  .then(async () => {
    if (!dataSource.isInitialized) await dataSource.initialize();
    await new OrgsAndGroupsSeeder(dataSource).run();
  })
  .catch(async (err) => {
    logger.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (dataSource.isInitialized) await dataSource.destroy();
  });
