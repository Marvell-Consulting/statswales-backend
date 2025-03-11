import { dataSource } from '../db/data-source';
import { Organisation } from '../entities/user/organisation';

export const OrganisationRepository = dataSource.getRepository(Organisation).extend({
  async listAll(): Promise<Organisation[]> {
    return this.find({ relations: { info: true } });
  }
});
