import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { CORE_VIEW_NAME } from '../services/cube-builder';
import { Locale } from '../enums/locale';
import { DatasetStats } from '../interfaces/dashboard-stats';

export interface ShareSourcesResult {
  sources: string[];
  datasets_count: number;
  datasets: string[];
  dataset_ids: string[];
  revision_ids: string[];
  dimensions_count?: number;
  dimensions?: string[];
  dimensions_common_count?: number;
  dimensions_common?: string[];
  topics_count?: number;
  topics?: string[];
}

export interface ShareDimensionsResult {
  dimensions: string[];
  datasets_count: number;
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
  async getDashboardStats(locale: Locale): Promise<DatasetStats> {
    logger.debug('Getting dashboard statistics for datasets');

    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';

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
      INNER JOIN revision_metadata rm ON rm.revision_id = r.id AND LOWER(rm.language) = $3
      GROUP BY r.dataset_id, rm.title, lt.row_count, lt.size_bytes
      ORDER BY lt.row_count DESC
      LIMIT 10;
    `,
      [`%${coreViewName}`, coreViewName, lang]
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
        INNER JOIN revision_metadata rm ON rm.revision_id = r.id AND LOWER(rm.language) = $1
        ORDER BY interval DESC
        LIMIT 10
      `,
      [lang]
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

  async shareSources(locale: Locale): Promise<ShareSourcesResult[]> {
    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';

    const sourceResults: ShareSourcesResult[] = await this.query(
      `
      WITH source_groups AS (
        SELECT
          r.dataset_id AS dataset_id,
          r.id AS revision_id,
          rm.title AS title,
          jsonb_agg(ps.name ORDER BY ps.name) AS sources
        FROM revision_provider rp
        JOIN revision r ON rp.revision_id = r.id
        JOIN revision_metadata rm ON rm.revision_id = r.id AND LOWER(rm.language) = $1
        JOIN provider_source ps ON rp.provider_source_id = ps.id AND LOWER(ps.language) = $1
        WHERE LOWER(rp.language) = $1
        AND rp.revision_id IN (${latestPublishedRevisionsQuery})
        GROUP BY r.dataset_id, r.id, rm.title
      ),
      grouped_sources AS (
        SELECT
          sources,
          COUNT(dataset_id) AS datasets_count,
          jsonb_agg(concat(title, ' [', dataset_id, ']')) AS datasets,
          jsonb_agg(dataset_id) AS dataset_ids,
          jsonb_agg(revision_id) AS revision_ids
        FROM source_groups
        GROUP BY sources
        HAVING COUNT(dataset_id) > 1
      ),
      all_dimensions AS (
        SELECT
          gs.sources,
          jsonb_agg(DISTINCT dm.name ORDER BY dm.name) AS dimensions,
          COUNT(DISTINCT dm.name) AS dimensions_count
        FROM grouped_sources gs
        JOIN LATERAL unnest(ARRAY(SELECT jsonb_array_elements_text(gs.revision_ids))::uuid[]) AS rev_id ON true
        JOIN dimension d ON d.dataset_id IN (SELECT jsonb_array_elements_text(gs.dataset_ids)::uuid)
        JOIN dimension_metadata dm ON dm.dimension_id = d.id AND LOWER(dm.language) = $1
        JOIN revision r ON r.dataset_id = d.dataset_id AND r.id = rev_id
        GROUP BY gs.sources
      ),
      common_dimensions AS (
        SELECT
          gs.sources,
          gs.datasets_count,
          dm.name AS dimension_name,
          COUNT(DISTINCT r.dataset_id) AS dataset_count
        FROM grouped_sources gs
        JOIN LATERAL unnest(ARRAY(SELECT jsonb_array_elements_text(gs.revision_ids))::uuid[]) AS rev_id ON true
        JOIN dimension d ON d.dataset_id IN (SELECT jsonb_array_elements_text(gs.dataset_ids)::uuid)
        JOIN dimension_metadata dm ON dm.dimension_id = d.id AND LOWER(dm.language) = $1
        JOIN revision r ON r.dataset_id = d.dataset_id AND r.id = rev_id
        GROUP BY gs.sources, gs.datasets_count, dm.name
        HAVING COUNT(DISTINCT r.dataset_id) = gs.datasets_count
      ),
      common_dimensions_agg AS (
        SELECT
          sources,
          jsonb_agg(dimension_name) AS dimensions_common,
          COUNT(dimension_name) AS dimensions_common_count
        FROM common_dimensions
        GROUP BY sources
      ),
      all_topics AS (
        SELECT
          gs.sources,
          jsonb_agg(DISTINCT t.name_en) AS topics,
          COUNT(DISTINCT t.name_en) AS topics_count
        FROM grouped_sources gs
        JOIN LATERAL unnest(ARRAY(SELECT jsonb_array_elements_text(gs.revision_ids))::uuid[]) AS rev_id ON true
        JOIN revision_topic rt ON rt.revision_id = rev_id
        JOIN topic t ON t.id = rt.topic_id
        GROUP BY gs.sources
      )
      SELECT
        gs.sources,
        gs.datasets_count,
        gs.datasets,
        gs.dataset_ids,
        gs.revision_ids,
        COALESCE(ad.dimensions_count, 0) AS dimensions_count,
        COALESCE(ad.dimensions, '[]'::jsonb) AS dimensions,
        COALESCE(cd.dimensions_common_count, 0) AS dimensions_common_count,
        COALESCE(cd.dimensions_common, '[]'::jsonb) AS dimensions_common,
        COALESCE(at.topics_count, 0) AS topics_count,
        COALESCE(at.topics, '[]'::jsonb) AS topics
      FROM grouped_sources gs
      LEFT JOIN all_dimensions ad ON ad.sources = gs.sources
      LEFT JOIN common_dimensions_agg cd ON cd.sources = gs.sources
      LEFT JOIN all_topics at ON at.sources = gs.sources
      ORDER BY gs.datasets_count DESC
      `,
      [lang]
    );

    const emptyResult: ShareSourcesResult = {
      sources: [],
      datasets_count: 0,
      datasets: [],
      dataset_ids: [],
      revision_ids: [],
      dimensions_count: 0,
      dimensions: [],
      topics_count: 0,
      topics: []
    };

    return sourceResults.length > 0 ? sourceResults : [emptyResult];
  },

  async shareDimensions(locale: Locale): Promise<ShareDimensionsResult[]> {
    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';

    const results: ShareDimensionsResult[] = await this.query(
      `
      SELECT
        dimensions,
        COUNT(dataset_id) AS datasets_count,
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
            JOIN revision_metadata rm ON rm.revision_id = r.id AND LOWER(rm.language) = $1
            JOIN dimension dim ON dim.dataset_id = r.dataset_id
            JOIN dimension_metadata dm ON dm.dimension_id = dim.id AND LOWER(dm.language) = $1
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
        datasets_count DESC
    `,
      [lang]
    );

    return results.length > 0 ? results : [{ dimensions: [], datasets_count: 0, datasets: [], dataset_ids: [] }];
  },

  async similarTitles(locale: Locale): Promise<SimilarTitlesResult[]> {
    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';

    await this.query(`SET pg_trgm.similarity_threshold = 0.6`);

    const results: SimilarTitlesResult[] = await this.query(
      `
      WITH latest_revisions AS (
        ${latestPublishedRevisionsQuery}
      )
      SELECT similarity(rm1.title, rm2.title) AS similarity_score, rm1.title AS title_1, rm2.title AS title_2
      FROM revision_metadata rm1
      JOIN revision_metadata rm2 ON rm1.revision_id <> rm2.revision_id
      AND rm1.title % rm2.title
      WHERE LOWER(rm1.language) = $1
        AND LOWER(rm2.language) = $1
        AND rm1.revision_id IN (SELECT id FROM latest_revisions)
        AND rm2.revision_id IN (SELECT id FROM latest_revisions)
      ORDER  BY similarity_score DESC`,
      [lang]
    );

    return results.length > 0 ? results : [{ similarity_score: 0, title_1: '', title_2: '' }];
  },

  async sameFactTable(locale: Locale): Promise<SameFactTableResult[]> {
    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';

    const results: SameFactTableResult[] = await this.query(
      `
      SELECT
        jsonb_agg(dt.original_filename) AS original_filenames,
        dt.hash AS datatable_hash,
        COUNT(r.id) AS count,
        jsonb_agg(concat(title, ' [', dataset_id, ']')) AS datasets
      FROM revision r
      JOIN revision_metadata rm ON rm.revision_id = r.id AND LOWER(rm.language) = $1
      JOIN data_table dt ON r.data_table_id = dt.id
      WHERE r.id IN (${latestPublishedRevisionsQuery})
      GROUP BY dt.hash
      HAVING COUNT(r.id) > 1
      ORDER BY COUNT(r.id) DESC
    `,
      [lang]
    );

    return results.length > 0 ? results : [{ original_filenames: [], datatable_hash: '', count: 0, datasets: [] }];
  }
});
