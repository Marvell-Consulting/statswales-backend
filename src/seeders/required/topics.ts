import 'dotenv/config';
import fs from 'node:fs';

import { parse } from 'csv-parse';
import { DataSource } from 'typeorm';

import { logger } from '../../utils/logger';
import { dataSource } from '../../db/data-source';
import { Topic } from '../../entities/dataset/topic';

interface CSVRow {
  id: string;
  path: string;
  l1_en: string;
  l1_cy: string;
  l2_en: string;
  l2_cy: string;
}

export class TopicsSeeder {
  constructor(private ds: DataSource) {
    this.ds = ds;
  }

  async run(): Promise<void> {
    logger.info('Starting TopicSeeder...');
    await this.seedTopics();
    logger.info('TopicSeeder finished.');
  }

  async seedTopics(): Promise<void> {
    const em = this.ds.createEntityManager();
    const csv = `${__dirname}/../../resources/topics/topics.csv`;
    const parserOpts = { delimiter: ',', bom: true, skip_empty_lines: true, columns: true };
    const topics: Topic[] = [];

    const parseCSV = async (): Promise<void> => {
      const csvParser: AsyncIterable<CSVRow> = fs.createReadStream(csv).pipe(parse(parserOpts));

      for await (const row of csvParser) {
        const topic = new Topic();
        topic.id = parseInt(row.id, 10);
        topic.path = row.path;
        topic.nameEN = row.id === row.path ? row.l1_en : row.l2_en;
        topic.nameCY = row.id === row.path ? row.l1_cy : row.l2_cy;
        topics.push(topic);
      }
      await em.save(Topic, topics);
    };

    await parseCSV();
    logger.info(`Seeded ${topics.length} topics`);
  }
}

Promise.resolve()
  .then(async () => {
    if (!dataSource.isInitialized) await dataSource.initialize();
    await new TopicsSeeder(dataSource).run();
  })
  .catch(async (err) => {
    logger.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (dataSource.isInitialized) await dataSource.destroy();
  });
