import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { CORE_VIEW_NAME } from '../services/cube-builder';
import { Locale } from '../enums/locale';
import { DatasetStats } from '../interfaces/dashboard-stats';

export interface ShareSourcesResult {
  sources: string[];
  dataset_count: number;
  datasets: string[];
  dataset_ids: string[];
  revision_ids: string[];
  dimension_count?: number;
  dimensions?: string[];
  topic_count?: number;
  topics?: string[];
}

export interface ShareDimensionsResult {
  dimensions: string[];
  dataset_count: number;
  datasets: string[];
  dataset_ids: string[];
}

export interface SimilarTitlesResult {
  title_1: string;
  title_2: string;
  similarity_score: number;
}

export interface SameFactTableResult {
  original_filenames: string[];
  datatable_hash: string;
  count: number;
  datasets: string[];
}

const latestPublishedRevisionsQuery = `
  SELECT DISTINCT ON (rev.dataset_id) rev.id
  FROM revision rev
  WHERE rev.approved_at IS NOT NULL
  AND rev.publish_at < NOW()
  ORDER BY rev.dataset_id, rev.created_at DESC
`;

export const DatasetStatsRepository = dataSource.getRepository(Dataset).extend({
  async getDashboardStats(lang: Locale): Promise<DatasetStats> {
    logger.debug('Getting dashboard statistics for datasets');

    const coreViewName = `${CORE_VIEW_NAME}_mat_en`;

    const statusQuery = this.query(`
      WITH dataset_stats AS (
        SELECT
          d.id,
          CASE
            WHEN d.archived_at IS NOT NULL AND d.archived_at < NOW() THEN 'archived'
            WHEN pr.unpublished_at IS NOT NULL AND pr.unpublished_at < NOW() THEN 'offline'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() THEN 'live'
            ELSE 'new'
          END as status,
          CASE
            WHEN d.first_published_at IS NOT NULL AND t.action = 'publish' AND t.status = 'requested' THEN 'update_pending_approval'
            WHEN t.action = 'publish' AND t.status = 'requested' THEN 'pending_approval'
            WHEN t.action = 'publish' AND t.status = 'rejected' THEN 'changes_requested'
            WHEN t.action = 'unpublish' AND t.status = 'requested' THEN 'unpublish_requested'
            WHEN t.action = 'archive' AND t.status = 'requested' THEN 'archive_requested'
            WHEN t.action = 'unarchive' AND t.status = 'requested' THEN 'unarchive_requested'
            WHEN pr.unpublished_at IS NOT NULL AND pr.unpublished_at < NOW() THEN 'unpublished'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NOT NULL AND r.publish_at < NOW() THEN 'published'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'update_scheduled'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at > NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'scheduled'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NULL THEN 'update_incomplete'
            WHEN d.first_published_at IS NULL AND r.approved_at IS NULL THEN 'incomplete'
            ELSE 'incomplete'
          END as publishing_status
        FROM dataset d
        INNER JOIN (
          SELECT DISTINCT ON (rev.dataset_id) rev.*
          FROM revision rev
          ORDER BY rev.dataset_id, rev.created_at DESC
        ) r ON r.dataset_id = d.id
        LEFT JOIN revision pr ON d.published_revision_id = pr.id
        LEFT JOIN task t ON d.id = t.dataset_id AND t.open = true
      )
      SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE publishing_status = 'published') as published,
        COUNT(*) FILTER (WHERE status = 'archived') as archived,
        COUNT(*) FILTER (WHERE status = 'offline') as offline,
        COUNT(*) FILTER (WHERE publishing_status = 'incomplete' OR publishing_status = 'update_incomplete') as incomplete,
        COUNT(*) FILTER (WHERE publishing_status = 'pending_approval' OR publishing_status = 'update_pending_approval') as pending_approval,
        COUNT(*) FILTER (WHERE publishing_status = 'scheduled' OR publishing_status = 'update_scheduled') as scheduled,
        COUNT(*) FILTER (WHERE publishing_status = 'unpublish_requested' OR publishing_status = 'archive_requested' OR publishing_status = 'unarchive_requested') as action_requested
      FROM dataset_stats
    `);

    const largestQuery = this.query(
      `
      WITH largest_tables AS (
        SELECT oid::regclass::text AS objectname, reltuples AS row_count, pg_relation_size(oid) AS size_bytes
        FROM pg_class
        WHERE relkind IN ('m')
        AND oid::regclass::text LIKE $1
        AND pg_relation_size(oid) > 0
        ORDER  BY reltuples DESC
      )
      SELECT
        r.dataset_id AS dataset_id,
        rm.title AS title,
        MAX(lt.row_count) AS row_count,
        MAX(lt.size_bytes) AS size_bytes
      FROM revision r
      INNER JOIN largest_tables lt ON '"'||r.id||'".'||$2 = lt.objectname
      INNER JOIN revision_metadata rm ON rm.revision_id = r.id AND rm.language LIKE $3
      GROUP BY r.dataset_id, rm.title, lt.row_count, lt.size_bytes
      ORDER BY lt.row_count DESC
      LIMIT 10;
    `,
      [`%${coreViewName}`, coreViewName, `${lang}%`]
    );

    const longestQuery = this.query(
      `
        SELECT r.dataset_id AS dataset_id, rm.title AS title,
        CASE
          WHEN r.approved_at IS NULL THEN EXTRACT(EPOCH FROM (NOW()::timestamp - r.created_at::timestamp))::int
          ELSE EXTRACT(EPOCH FROM (r.approved_at::timestamp - r.created_at::timestamp))::int
        END AS interval,
        CASE
          WHEN r.approved_at IS NOT NULL AND r.publish_at < NOW() THEN 'published'
          WHEN r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'scheduled'
          ELSE 'incomplete'
        END AS status
        FROM revision r
        INNER JOIN revision_metadata rm ON rm.revision_id = r.id AND rm.language LIKE $1
        ORDER BY interval DESC
        LIMIT 10
      `,
      [`${lang}%`]
    );

    const [status, largest, longest] = await Promise.all([statusQuery, largestQuery, longestQuery]);

    const summary = {
      incomplete: Number(status[0].incomplete),
      pending_approval: Number(status[0].pending_approval),
      scheduled: Number(status[0].scheduled),
      published: Number(status[0].published),
      action_requested: Number(status[0].action_requested),
      archived: Number(status[0].archived),
      offline: Number(status[0].offline),
      total: Number(status[0].total)
    };

    return { summary, largest, longest };
  },

  async shareSources(): Promise<ShareSourcesResult[]> {
    const sourceResults = await this.query(`
      SELECT
        sources,
        COUNT(dataset_id) AS dataset_count,
        jsonb_agg(concat(title, ' [', dataset_id, ']')) AS datasets,
        jsonb_agg(dataset_id) AS dataset_ids,
        jsonb_agg(revision_id) AS revision_ids
      FROM (
        SELECT
          r.dataset_id AS dataset_id,
          r.id AS revision_id,
          rm.title AS title,
          jsonb_agg(ps.name) AS sources
        FROM revision_provider rp
        JOIN revision r ON rp.revision_id = r.id
        JOIN revision_metadata rm ON rm.revision_id = r.id AND rm."language" = 'en-GB'
        JOIN provider_source ps ON rp.provider_source_id = ps.id AND ps."language" = 'en-gb'
        WHERE rp."language" = 'en-gb'
        AND rp.revision_id IN (${latestPublishedRevisionsQuery})
        GROUP BY r.dataset_id, r.id, rm.title
      )
      GROUP BY sources
      HAVING COUNT(dataset_id) > 1
      ORDER BY dataset_count DESC`);

    for (const result of sourceResults) {
      const dimensions = await this.query(`
        SELECT
          jsonb_agg(DISTINCT dm.name) AS dimensions,
          jsonb_agg(DISTINCT t.name_en) AS topics
        FROM dimension d
        JOIN dimension_metadata dm ON dm.dimension_id = d.id AND dm."language" = 'en-GB'
        JOIN revision r ON r.dataset_id = d.dataset_id
        JOIN revision_topic rt ON rt.revision_id = r.id
        JOIN topic t ON t.id = rt.topic_id
        WHERE r.id IN ('${result.revision_ids.join("', '")}')
      `);
      result.dimension_count = dimensions[0].dimensions.length;
      result.dimensions = dimensions[0].dimensions;
      result.topic_count = dimensions[0].topics.length;
      result.topics = dimensions[0].topics;
    }

    return sourceResults;
  },

  async shareDimensions(): Promise<ShareDimensionsResult[]> {
    const results = await this.query(`
      SELECT
        dimensions,
        COUNT(dataset_id) AS dataset_count,
        jsonb_agg(concat(title, ' [', dataset_id, ']')) AS datasets,
        jsonb_agg(dataset_id) AS dataset_ids
      FROM
        (
          SELECT
            r.dataset_id AS dataset_id,
            r.id AS revision_id,
            rm.title AS title,
            jsonb_agg(dm.name) AS dimensions
          FROM
            revision r
            JOIN revision_metadata rm ON rm.revision_id = r.id AND rm."language" = 'en-GB'
            JOIN dimension dim ON dim.dataset_id = r.dataset_id
            JOIN dimension_metadata dm ON dm.dimension_id = dim.id AND dm."language" = 'en-GB'
          WHERE r.id IN (
            ${latestPublishedRevisionsQuery}
            )
          GROUP BY r.dataset_id, r.id, rm.title
          HAVING COUNT(dim.id) > 1
        )
      GROUP BY dimensions
      HAVING
        COUNT(dataset_id) > 1
      ORDER BY
        dataset_count DESC
    `);

    return results;
  },

  async similarTitles(): Promise<SimilarTitlesResult[]> {
    await this.query(`SET pg_trgm.similarity_threshold = 0.6`);

    const results = await this.query(`
      WITH latest_revisions AS (
        ${latestPublishedRevisionsQuery}
      )
      SELECT similarity(rm1.title, rm2.title) AS similarity_score, rm1.title AS title_1, rm2.title AS title_2
      FROM revision_metadata rm1
      JOIN revision_metadata rm2 ON rm1.revision_id <> rm2.revision_id
      AND rm1.title % rm2.title
      WHERE rm1."language" = 'en-GB'
        AND rm2."language" = 'en-GB'
        AND rm1.revision_id IN (SELECT id FROM latest_revisions)
        AND rm2.revision_id IN (SELECT id FROM latest_revisions)
      ORDER  BY similarity_score DESC`);

    return results;
  },

  async sameFactTable(): Promise<SameFactTableResult[]> {
    const results = await this.query(`
      SELECT
        jsonb_agg(dt.original_filename) AS original_filenames,
        dt.hash AS datatable_hash,
        COUNT(r.id) AS count,
        jsonb_agg(concat(title, ' [', dataset_id, ']')) AS datasets
      FROM revision r
      JOIN revision_metadata rm ON rm.revision_id = r.id AND rm."language" = 'en-GB'
      JOIN data_table dt ON r.data_table_id = dt.id
      WHERE r.id IN (${latestPublishedRevisionsQuery})
      GROUP BY dt.hash
      HAVING COUNT(r.id) > 1
      ORDER BY COUNT(r.id) DESC
    `);

    return results;
  }
});
