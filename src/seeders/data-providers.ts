import fs from 'node:fs';
import { randomUUID } from 'node:crypto';

import { parse } from 'csv';
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { Provider } from '../entities/dataset/provider';
import { logger } from '../utils/logger';
import { ProviderSource } from '../entities/dataset/provider-source';

export default class DataProviderSeeder extends Seeder {
    async run(dataSource: DataSource): Promise<void> {
        await this.seedProviders(dataSource);
        await this.loadProviderSources(dataSource);
    }

    async seedProviders(dataSource: DataSource) {
        logger.info('Seeding providers...');
        const providerCSV = `${__dirname}/../resources/data-providers/providers.csv`;
        const providers: Provider[] = [];

        const parseCSV = async () => {
            const parser = fs
                .createReadStream(providerCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));

            for await (const row of parser) {
                const provider = new Provider();
                provider.id = randomUUID().toLowerCase();
                provider.name = row.name;
                provider.language = row.language;
                providers.push(provider);
            }
        };

        await parseCSV();
        await dataSource.createEntityManager().save<Provider>(providers);
        logger.info(`Seeded ${providers.length} providers`);
    }

    async loadProviderSources(dataSource: DataSource) {
        logger.info('Seeding provider sources...');
        const providerSourcesCSV = `${__dirname}/../resources/data-providers/provider_sources.csv`;
        const providerSources: ProviderSource[] = [];
        const providers = await dataSource.createEntityManager().find<Provider>(Provider);

        const parseCSV = async () => {
            const parser = fs
                .createReadStream(providerSourcesCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));

            for await (const row of parser) {
                const providerSource = new ProviderSource();
                providerSource.id = randomUUID().toLowerCase();
                providerSource.name = row.name;
                providerSource.language = row.language;
                providerSource.provider = providers.find((provider) => provider.name === row.provider_name)!;
                providerSources.push(providerSource);
            }
        };

        await parseCSV();
        await dataSource.createEntityManager().save<ProviderSource>(providerSources);
        logger.info(`Seeded ${providerSources.length} provider sources`);
    }
}
