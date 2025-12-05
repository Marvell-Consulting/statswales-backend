import 'dotenv/config';
import { DataSource, DeepPartial } from 'typeorm';

import { logger } from '../../utils/logger';
import { dataSource } from '../../db/data-source';
import { Provider } from '../../entities/dataset/provider';
import { ProviderSource } from '../../entities/dataset/provider-source';
import providers from '../../resources/data-providers/provider.json';
import providerSources from '../../resources/data-providers/provider_source.json';

export class DataProviderSeeder {
  async run(): Promise<void> {
    logger.info('Starting DataProviderSeeder...');

    await dataSource.initialize();
    await this.seedProviders(dataSource);
    await this.seedProviderSources(dataSource);
    await dataSource.destroy();

    logger.info('DataProviderSeeder finished.');
  }

  async seedProviders(datasource: DataSource): Promise<void> {
    logger.info('Seeding providers...');
    const em = datasource.createEntityManager();
    const savedProviders = await em.save(Provider, providers);
    logger.info(`Seeded ${savedProviders.length} providers`);
  }

  async seedProviderSources(dataSource: DataSource): Promise<void> {
    logger.info('Seeding provider sources...');
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

Promise.resolve()
  .then(async () => {
    const seeder = new DataProviderSeeder();
    await seeder.run();
  })
  .catch(async (err) => {
    logger.error(err);
    await dataSource.destroy();
    process.exit(1);
  });
