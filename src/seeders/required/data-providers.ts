import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource, DeepPartial } from 'typeorm';

import { Provider } from '../../entities/dataset/provider';
import { logger } from '../../utils/logger';
import { ProviderSource } from '../../entities/dataset/provider-source';
import providers from '../../resources/data-providers/provider.json';
import providerSources from '../../resources/data-providers/provider_source.json';

export default class DataProviderSeeder extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    await this.seedProviders(dataSource);
    await this.seedProviderSources(dataSource);
  }

  async seedProviders(datasource: DataSource): Promise<void> {
    const em = datasource.createEntityManager();
    const savedProviders = await em.save(Provider, providers);
    logger.info(`Seeded ${savedProviders.length} providers`);
  }

  async seedProviderSources(dataSource: DataSource): Promise<void> {
    const em = dataSource.createEntityManager();
    const sources: DeepPartial<ProviderSource>[] = providerSources.map((pSource) => ({
      id: pSource.id,
      sw2Id: pSource.sw2_id,
      providerId: pSource.provider_id,
      language: pSource.language,
      name: pSource.name
    }));
    const savedProviderSources = await em.save(ProviderSource, sources);
    logger.info(`Seeded ${savedProviderSources.length} provider sources`);
  }
}
