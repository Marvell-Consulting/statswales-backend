import fs from 'node:fs';

import { parse } from 'csv';
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { logger } from '../utils/logger';
import { Topic } from '../entities/dataset/topic';

interface CSVRow {
    id: string;
    path: string;
    l1_en: string;
    l1_cy: string;
    l2_en: string;
    l2_cy: string;
}

export default class TopicSeeder extends Seeder {
    async run(dataSource: DataSource): Promise<void> {
        await this.seedTopics(dataSource);
    }

    async seedTopics(dataSource: DataSource): Promise<void> {
        const em = dataSource.createEntityManager();
        const csv = `${__dirname}/../resources/topics/topics.csv`;
        const parserOpts = { delimiter: ',', bom: true, skip_empty_lines: true, columns: true };
        const topics: Topic[] = [];

        const parseCSV = async () => {
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
