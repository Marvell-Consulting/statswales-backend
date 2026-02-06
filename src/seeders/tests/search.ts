import 'dotenv/config';
import { DataSource } from 'typeorm';

import { dataSource } from '../../db/data-source';
import { config } from '../../config';
import { AppEnv } from '../../config/env.enum';
import { logger } from '../../utils/logger';

import { UserGroup } from '../../entities/user/user-group';
import { group1 } from './fixtures/group';

import { Dataset } from '../../entities/dataset/dataset';
import { approver1, publisher1 } from './fixtures/users';
import { Revision } from '../../entities/dataset/revision';
import { User } from '../../entities/user/user';
import { RevisionMetadata } from '../../entities/dataset/revision-metadata';
import { Locale } from '../../enums/locale';
import { DatasetDTO } from '../../dtos/dataset-dto';
import { RevisionMetadataDTO } from '../../dtos/revistion-metadata-dto';
import { DatasetListItemDTO } from '../../dtos/dataset-list-item-dto';
import { ResultsetWithCount } from '../../interfaces/resultset-with-count';
import { uuidV4 } from '../../utils/uuid';

const prodApiUrl = 'https://api.stats.gov.wales/v1/';

// Delay between API requests to stay under the 100 req/min rate limit
const API_DELAY_MS = 1000;

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// This seeder loads test fixtures used for testing search quality.
export class SearchSeeder {
  constructor(private ds: DataSource) {}

  async fetchDatasetList(): Promise<ResultsetWithCount<DatasetListItemDTO>> {
    const response = await fetch(prodApiUrl + '?lang=en-gb&page_size=100000');
    if (!response.ok) {
      throw new Error(`Failed to fetch all datasets: ${response.status} ${response.statusText}`);
    }

    return (await response.json()) as ResultsetWithCount<DatasetListItemDTO>;
  }

  async run(): Promise<void> {
    if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
      throw new Error('SearchSeeder is only intended to be run in local or test environments');
    }

    logger.info('Starting SearchSeeder...');
    const liveDatasets = await this.fetchDatasetList();
    await this.seedDatasets(liveDatasets);
    logger.info('SearchSeeder finished.');
  }

  async seedDatasets(liveDatasets: ResultsetWithCount<DatasetListItemDTO>): Promise<void> {
    logger.info(`Seeding ${liveDatasets.count} datasets for search quality tests...`);

    const publisher = await this.ds.getRepository(User).findOneByOrFail({ id: publisher1.id });
    const approver = await this.ds.getRepository(User).findOneByOrFail({ id: approver1.id });
    const group = await this.ds.getRepository(UserGroup).findOneByOrFail({ id: group1.id });
    const datasets: Dataset[] = [];

    for (const liveDataset of liveDatasets.data) {
      let dataset: Dataset;
      let publishedRevMeta: RevisionMetadataDTO[] | undefined;
      const firstPublishedAt = new Date(liveDataset.first_published_at || Date.now());

      try {
        logger.debug(`Fetching published revision metadata for dataset ${liveDataset.id}...`);
        const response = await fetch(`${prodApiUrl}${liveDataset.id}?lang=en-gb`);

        if (!response.ok) {
          throw new Error(`Failed: ${response.status} ${response.statusText}`);
        }

        const fullDatasetDTO = (await response.json()) as DatasetDTO;
        publishedRevMeta = fullDatasetDTO.published_revision?.metadata;

        dataset = await Dataset.create({
          id: liveDataset.id,
          createdBy: publisher,
          userGroup: group,
          firstPublishedAt
        }).save();

        const revision = await Revision.create({
          id: fullDatasetDTO.published_revision?.id || uuidV4(),
          revisionIndex: 1,
          datasetId: liveDataset.id,
          createdBy: publisher,
          publishAt: firstPublishedAt,
          approvedAt: firstPublishedAt,
          approvedBy: approver,
          metadata: [
            RevisionMetadata.create({
              language: Locale.EnglishGb,
              title: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.title,
              summary: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.summary,
              collection: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.collection,
              quality: publishedRevMeta?.find((meta) => meta.language === Locale.EnglishGb)?.quality
            }),
            RevisionMetadata.create({
              language: Locale.WelshGb,
              title: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.title,
              summary: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.summary,
              collection: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.collection,
              quality: publishedRevMeta?.find((meta) => meta.language === Locale.WelshGb)?.quality
            })
          ]
        }).save();

        dataset = await Dataset.merge(dataset, {
          startRevision: revision,
          endRevision: revision,
          publishedRevision: revision
        }).save();

        datasets.push(dataset);
        await delay(API_DELAY_MS);
      } catch (err) {
        logger.warn(`Failed to fetch metadata for dataset ${liveDataset.id}: ${err}`);
        // skip dataset and continue with the next one
      }
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
