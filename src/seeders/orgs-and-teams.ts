import fs from 'node:fs';

import { parse } from 'csv';
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource, DeepPartial } from 'typeorm';

import { logger } from '../utils/logger';
import { Organisation } from '../entities/user/organisation';
import { Team } from '../entities/user/team';

// using hard coded uuids so that we can re-run the seeder for updates without creating new records
const organisations: DeepPartial<Organisation>[] = [
  {
    id: '4ef4facf-c488-4837-a65b-e66d4b525965',
    info: [
      { name: 'Welsh Government', language: 'en' },
      { name: 'Llywodraeth Cymru', language: 'cy' }
    ]
  },
  {
    id: '51326112-33c1-4bdf-b51f-76f630ef4c48',
    info: [
      { name: 'Welsh Revenue Authority', language: 'en' },
      { name: 'Awdurdod Cyllid Cymru', language: 'cy' }
    ]
  }
];

interface CSVRow {
  id: string;
  name_en: string;
  name_cy: string;
  prefix: string;
  email_en: string;
  email_cy: string;
  organisation_en: string;
  organisation_cy: string;
}

export default class OrgsAndTeamsSeeder extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    const orgs = await this.seedOrganisations(dataSource);
    await this.seedTeams(dataSource, orgs);
  }

  async seedOrganisations(dataSource: DataSource): Promise<Organisation[]> {
    const savedOrgs = await dataSource.getRepository(Organisation).save(organisations);
    logger.info(`Seeded ${savedOrgs.length} organisations`);
    return savedOrgs;
  }

  async seedTeams(dataSource: DataSource, organisations: Organisation[]): Promise<void> {
    const csv = `${__dirname}/../resources/teams/teams.csv`;
    const parserOpts = { delimiter: ',', bom: true, skip_empty_lines: true, columns: true };

    const parseCSV = async () => {
      const csvParser: AsyncIterable<CSVRow> = fs.createReadStream(csv).pipe(parse(parserOpts));
      const teams: DeepPartial<Team>[] = [];

      for await (const row of csvParser) {
        const team: DeepPartial<Team> = {
          id: row.id,
          prefix: row.prefix,
          organisation: organisations.find((org) => org.info[0].name === row.organisation_en),
          info: [
            { name: row.name_en.trim(), email: row.email_en.trim(), language: 'en' },
            { name: row.name_cy.trim(), email: row.email_cy.trim(), language: 'cy' }
          ]
        };
        teams.push(team);
      }

      await dataSource.getRepository(Team).save(teams);
      logger.info(`Seeded ${teams.length} teams`);
    };

    await parseCSV();
  }
}
