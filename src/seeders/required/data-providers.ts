import 'dotenv/config';
import { DataSource, DeepPartial } from 'typeorm';

import { logger } from '../../utils/logger';
import { dataSource } from '../../db/data-source';
import { Provider } from '../../entities/dataset/provider';
import { ProviderSource } from '../../entities/dataset/provider-source';
import providers from '../../resources/data-providers/provider.json';
import providerSources from '../../resources/data-providers/provider_source.json';

export class DataProviderSeeder {
  constructor(private ds: DataSource) {
    this.ds = ds;
  }

  async run(): Promise<void> {
    logger.info('Starting DataProviderSeeder...');
    await this.seedProviders();
    await this.seedProviderSources();
    logger.info('DataProviderSeeder finished.');
  }

  async seedProviders(): Promise<void> {
    logger.info('Seeding providers...');
    const em = this.ds.createEntityManager();
    const savedProviders = await em.save(Provider, providers);
    logger.info(`Seeded ${savedProviders.length} providers`);
  }

  async seedProviderSources(): Promise<void> {
    logger.info('Seeding provider sources...');
    const em = this.ds.createEntityManager();
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
    if (!dataSource.isInitialized) await dataSource.initialize();
    await new DataProviderSeeder(dataSource).run();
  })
  .catch(async (err) => {
    logger.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (dataSource.isInitialized) await dataSource.destroy();
  });
