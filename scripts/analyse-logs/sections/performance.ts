import { query } from '../duckdb-helper';
import { heading, table } from '../markdown';

export async function performance(): Promise<string> {
  const lines: string[] = [heading(2, 'Performance')];

  const byRoute = await query(`
    SELECT
      method,
      route,
      count(*) AS cnt,
      round(quantile_cont(response_time, 0.5)::DOUBLE, 1) AS p50,
      round(quantile_cont(response_time, 0.95)::DOUBLE, 1) AS p95,
      round(quantile_cont(response_time, 0.99)::DOUBLE, 1) AS p99
    FROM logs
    WHERE response_time IS NOT NULL
      AND url NOT LIKE '%/healthcheck%'
    GROUP BY method, route
    HAVING count(*) >= 10
    ORDER BY p95 DESC
    LIMIT 20
  `);
  lines.push(heading(3, 'Slowest Routes by p95 (min 10 requests)'));
  lines.push(
    table(
      ['Method', 'Route', 'Count', 'p50 (ms)', 'p95 (ms)', 'p99 (ms)'],
      byRoute.map((r) => [
        r.method as string,
        r.route as string,
        Number(r.cnt).toLocaleString(),
        r.p50 as number,
        r.p95 as number,
        r.p99 as number
      ])
    )
  );

  const byDataset = await query(`
    WITH dataset_requests AS (
      SELECT
        regexp_extract(url, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', 0) AS dataset_id,
        response_time
      FROM logs
      WHERE response_time IS NOT NULL AND url IS NOT NULL
    )
    SELECT
      dataset_id,
      count(*) AS cnt,
      round(quantile_cont(response_time, 0.5)::DOUBLE, 1) AS p50,
      round(quantile_cont(response_time, 0.95)::DOUBLE, 1) AS p95
    FROM dataset_requests
    WHERE dataset_id IS NOT NULL AND dataset_id != ''
    GROUP BY dataset_id
    HAVING count(*) >= 5
    ORDER BY p95 DESC
    LIMIT 15
  `);
  lines.push(heading(3, 'Slowest Datasets by p95 (min 5 requests)'));
  lines.push(
    table(
      ['Dataset ID', 'Count', 'p50 (ms)', 'p95 (ms)'],
      byDataset.map((r) => [r.dataset_id as string, Number(r.cnt).toLocaleString(), r.p50 as number, r.p95 as number])
    )
  );

  const thresholds = await query(`
    SELECT
      count(*) FILTER (WHERE response_time > 5000) AS over_5s,
      count(*) FILTER (WHERE response_time > 10000) AS over_10s,
      count(*) FILTER (WHERE response_time > 30000) AS over_30s
    FROM logs
    WHERE response_time IS NOT NULL
  `);
  lines.push(heading(3, 'Slow Request Thresholds'));
  lines.push(
    table(
      ['Threshold', 'Count'],
      [
        ['> 5 seconds', Number(thresholds[0].over_5s).toLocaleString()],
        ['> 10 seconds', Number(thresholds[0].over_10s).toLocaleString()],
        ['> 30 seconds', Number(thresholds[0].over_30s).toLocaleString()]
      ]
    )
  );

  return lines.join('\n');
}
