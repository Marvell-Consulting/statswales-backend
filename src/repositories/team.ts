import { FindManyOptions } from 'typeorm';

import { Locale } from '../enums/locale';
import { dataSource } from '../db/data-source';
import { Team } from '../entities/user/team';

export const TeamRepository = dataSource.getRepository(Team).extend({
    async getById(id: string): Promise<Team> {
        return this.findOneOrFail({
            where: { id },
            relations: { organisation: true }
        });
    },

    async listAll(locale: Locale): Promise<Team[]> {
        const findOpts: FindManyOptions<Team> = {
            relations: { organisation: true },
            order: locale.includes('en') ? { nameEN: 'ASC' } : { nameCY: 'ASC' }
        };
        return this.find(findOpts);
    }
});
