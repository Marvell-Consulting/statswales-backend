import { publisherDataSource } from '../db/publisher-source';
import { Organisation } from '../entities/user/organisation';

export const OrganisationRepository = publisherDataSource.getRepository(Organisation).extend({
  async listAll(): Promise<Organisation[]> {
    return this.find({ relations: { metadata: true } });
  }
});
