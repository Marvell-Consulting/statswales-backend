import 'dotenv/config';
import { DataSource } from 'typeorm';

import { dataSource } from '../../db/data-source';
import { config } from '../../config';
import { AppEnv } from '../../config/env.enum';
import { logger } from '../../utils/logger';

import { UserGroup } from '../../entities/user/user-group';
import { group1 } from './fixtures/group';

import realDatasets from './fixtures/published-datasets.json';
import { Dataset } from '../../entities/dataset/dataset';
import { approver1, publisher1 } from './fixtures/users';
import { Revision } from '../../entities/dataset/revision';
import { User } from '../../entities/user/user';
import { RevisionMetadata } from '../../entities/dataset/revision-metadata';
import { Locale } from '../../enums/locale';
import { DatasetDTO } from '../../dtos/dataset-dto';
import { RevisionMetadataDTO } from '../../dtos/revistion-metadata-dto';

const prodApiUrl = 'https://api.stats.gov.wales/v1/';

// Delay between API requests to stay under the 100 req/min rate limit
const API_DELAY_MS = 650;
const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// This seeder loads test fixtures used for testing search quality.
export class SearchSeeder {
  constructor(private ds: DataSource) {}

  async run(): Promise<void> {
    if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
      throw new Error('SearchSeeder is only intended to be run in local or test environments');
    }

    logger.info('Starting SearchSeeder...');
    await this.seedDatasets();
    logger.info('SearchSeeder finished.');
  }

  async seedDatasets(): Promise<void> {
    logger.info(`Seeding ${realDatasets.count} datasets for search quality tests...`);

    const publisher = await this.ds.getRepository(User).findOneByOrFail({ id: publisher1.id });
    const approver = await this.ds.getRepository(User).findOneByOrFail({ id: approver1.id });
    const group = await this.ds.getRepository(UserGroup).findOneByOrFail({ id: group1.id });
    const datasets: Dataset[] = [];

    for (const real of realDatasets.data) {
      const dataset = await Dataset.create({
        id: real.id,
        createdBy: publisher,
        userGroup: group,
        firstPublishedAt: new Date(real.first_published_at)
      }).save();

      let publishedRevMeta: RevisionMetadataDTO[] | undefined;

      try {
        logger.debug(`Fetching published revision metadata for dataset ${real.id}...`);
        const response = await fetch(`${prodApiUrl}${real.id}?lang=en-gb`);

        if (response.ok) {
          const apiData = (await response.json()) as DatasetDTO;
          publishedRevMeta = apiData.published_revision?.metadata;
        }
      } catch (err) {
        logger.warn(`Failed to fetch metadata for dataset ${real.id}: ${err}`);
      }

      // Delay to respect API rate limit (100 req/min)
      await delay(API_DELAY_MS);

      const revision = await Revision.create({
        revisionIndex: 1,
        datasetId: dataset.id,
        createdBy: publisher,
        publishAt: new Date(real.first_published_at),
        approvedAt: new Date(real.first_published_at),
        approvedBy: approver,
        metadata: [
          RevisionMetadata.create({
            language: Locale.EnglishGb,
            title: real.title_en,
            summary: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.summary,
            collection: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.collection,
            quality: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.quality
          }),
          RevisionMetadata.create({
            language: Locale.WelshGb,
            title: real.title_cy,
            summary: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.summary,
            collection: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.collection,
            quality: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.quality
          })
        ]
      }).save();

      await Dataset.merge(dataset, {
        startRevision: revision,
        endRevision: revision,
        publishedRevision: revision
      }).save();

      datasets.push(dataset);
    }

    logger.info(`Saved ${datasets.length} datasets for search quality tests.`);
  }
}

Promise.resolve()
  .then(async () => {
    if (!dataSource.isInitialized) await dataSource.initialize();
    await new SearchSeeder(dataSource).run();
  })
  .catch(async (err) => {
    logger.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (dataSource.isInitialized) await dataSource.destroy();
  });
