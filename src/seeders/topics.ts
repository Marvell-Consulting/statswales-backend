import fs from 'node:fs';

import { parse } from 'csv';
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { logger } from '../utils/logger';
import { Topic } from '../entities/dataset/topic';

interface CSVRow {
    L1: string;
    L2: string;
}

export default class TopicSeeder extends Seeder {
    async run(dataSource: DataSource): Promise<void> {
        await this.seedTopics(dataSource);
    }

    async seedTopics(dataSource: DataSource): Promise<void> {
        const em = dataSource.createEntityManager();
        const csv = `${__dirname}/../resources/topics/topics.csv`;
        const parserOpts = { delimiter: ',', bom: true, skip_empty_lines: true, columns: true };
        const rootTopics: Topic[] = [];
        const childTopics: Topic[] = [];
        let topicId = 1;

        const parseCSV = async () => {
            const csvParser: AsyncIterable<CSVRow> = fs.createReadStream(csv).pipe(parse(parserOpts));
            let rootTopic = Topic.create();

            for await (const row of csvParser) {
                if (row.L1) {
                    rootTopic = Topic.create();
                    rootTopic.id = topicId;
                    rootTopic.path = topicId.toString();
                    rootTopic.nameEN = row.L1.trim();
                    rootTopics.push(rootTopic);
                    topicId++;
                    await em.save<Topic>(rootTopic);
                    logger.info(`Seeded root topic '${rootTopic.nameEN}'`);
                }

                if (row.L2) {
                    const childTopic = Topic.create();
                    childTopic.id = topicId;
                    childTopic.path = `${rootTopic.path}.${topicId}`;
                    childTopic.nameEN = row.L2.trim();
                    childTopics.push(childTopic);
                    topicId++;
                    await em.save<Topic>(childTopic);
                    logger.info(`Seeded child topic '${childTopic.nameEN}'`);
                }
            }
        };

        await parseCSV();
        logger.info(`Seeded ${rootTopics.length} root topics and ${childTopics.length} child topics`);
    }
}
