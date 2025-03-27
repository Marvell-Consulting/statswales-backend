import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource, DeepPartial } from 'typeorm';

import { logger } from '../utils/logger';
import { Organisation } from '../entities/user/organisation';
import { Locale } from '../enums/locale';

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

export default class OrganisationSeeder extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    await this.seedOrganisations(dataSource);
  }

  async seedOrganisations(dataSource: DataSource): Promise<Organisation[]> {
    const savedOrgs = await dataSource.getRepository(Organisation).save(organisations);
    logger.info(`Seeded ${savedOrgs.length} organisations`);
    return savedOrgs;
  }
}
