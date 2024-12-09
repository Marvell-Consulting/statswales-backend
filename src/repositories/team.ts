import { FindManyOptions } from 'typeorm';

import { dataSource } from '../db/data-source';
import { Team } from '../entities/user/team';

export const TeamRepository = dataSource.getRepository(Team).extend({
    async getById(id: string): Promise<Team> {
        return this.findOneOrFail({
            where: { id },
            relations: { info: true, organisation: true }
        });
    },

    async listAll(): Promise<Team[]> {
        const findOpts: FindManyOptions<Team> = {
            relations: { info: true, organisation: { info: true } }
        };
        return this.find(findOpts);
    }
});
