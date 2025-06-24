import fs from 'node:fs';

import { parse } from 'csv';
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';
import { v4 as uuid } from 'uuid';

import { Provider } from '../../entities/dataset/provider';
import { logger } from '../../utils/logger';
import { ProviderSource } from '../../entities/dataset/provider-source';
import { Locale } from '../../enums/locale';

interface CSVRow {
  original_id: string;
  provider_name_en: string;
  provider_name_cy: string;
  source_name_en: string;
  source_name_cy: string;
}

export default class DataProviderSeeder extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    await this.seedProvidersAndSources(dataSource);
  }

  async seedProvidersAndSources(dataSource: DataSource): Promise<void> {
    const csv = `${__dirname}/../../resources/data-providers/provider_sources.csv`;
    const parserOpts = { delimiter: ',', bom: true, skip_empty_lines: true, columns: true };
    const providers: Provider[] = [];
    const providerSources: ProviderSource[] = [];
    const english = Locale.EnglishGb.toLowerCase();
    const welsh = Locale.WelshGb.toLowerCase();

    const parseCSV = async (): Promise<void> => {
      const csvParser: AsyncIterable<CSVRow> = fs.createReadStream(csv).pipe(parse(parserOpts));

      for await (const row of csvParser) {
        const providerSourceId = uuid();
        const originalId = row.original_id?.trim() ? parseInt(row.original_id.trim(), 10) : undefined;

        const providerSourceEN = new ProviderSource();
        providerSourceEN.id = providerSourceId;
        providerSourceEN.sw2_id = originalId;
        providerSourceEN.name = row.source_name_en;
        providerSourceEN.language = english;

        const providerSourceCY = new ProviderSource();
        providerSourceCY.id = providerSourceId;
        providerSourceCY.sw2_id = originalId;
        providerSourceCY.name = row.source_name_cy;
        providerSourceCY.language = welsh;

        const existingProviderEN = providers.find(
          (provider) => provider.language === english && provider.name === row.provider_name_en
        );

        if (existingProviderEN) {
          providerSourceEN.provider = existingProviderEN;
          providerSourceCY.provider = providers.find(
            (provider) => provider.language === welsh && provider.name === row.provider_name_cy
          )!;
        } else {
          const providerId = uuid();

          const providerEN = new Provider();
          providerEN.id = providerId;
          providerEN.name = row.provider_name_en;
          providerEN.language = english;
          providers.push(providerEN);
          providerSourceEN.provider = providerEN;

          const providerCY = new Provider();
          providerCY.id = providerId;
          providerCY.name = row.provider_name_cy;
          providerCY.language = welsh;
          providers.push(providerCY);
          providerSourceCY.provider = providerCY;
        }

        providerSources.push(providerSourceEN, providerSourceCY);
      }
    };

    await parseCSV();
    await dataSource.createEntityManager().save<ProviderSource>(providerSources);
    logger.info(`Seeded ${providers.length / 2} providers and ${providerSources.length / 2} provider sources`);
  }
}
