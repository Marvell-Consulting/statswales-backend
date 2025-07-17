import { appDataSource } from '../db/data-source';
import { Organisation } from '../entities/user/organisation';

export const OrganisationRepository = appDataSource.getRepository(Organisation).extend({
  async listAll(): Promise<Organisation[]> {
    return this.find({ relations: { metadata: true } });
  }
});
