import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { logger } from '../utils/logger';
import { Organisation } from '../entities/user/organisation';
import { Team } from '../entities/user/team';
import { organisations, teams } from '../resources/teams/orgs-and-teams';

export default class OrgsAndTeamsSeeder extends Seeder {
    async run(dataSource: DataSource): Promise<void> {
        const savedOrgs = await dataSource.getRepository(Organisation).save(organisations);
        const savedTeams = await dataSource.getRepository(Team).save(teams);
        logger.info(`Seeded ${savedOrgs.length} organisations and ${savedTeams.length} teams`);
    }
}
