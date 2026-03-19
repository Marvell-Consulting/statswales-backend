import { query } from '../duckdb-helper';
import { heading, table } from '../markdown';

export async function statusCodes(): Promise<string> {
  const lines: string[] = [heading(2, 'Status Codes')];

  const summary = await query(`
    SELECT
      CASE
        WHEN status_code BETWEEN 200 AND 299 THEN '2xx'
        WHEN status_code BETWEEN 300 AND 399 THEN '3xx'
        WHEN status_code BETWEEN 400 AND 499 THEN '4xx'
        WHEN status_code BETWEEN 500 AND 599 THEN '5xx'
        ELSE 'other'
      END AS category,
      count(*) AS cnt
    FROM logs
    WHERE status_code IS NOT NULL
      AND url NOT LIKE '%/healthcheck%'
    GROUP BY category
    ORDER BY category
  `);
  lines.push(heading(3, 'Status Code Summary (excl. healthchecks)'));
  lines.push(
    table(
      ['Category', 'Count'],
      summary.map((r) => [r.category as string, Number(r.cnt).toLocaleString()])
    )
  );

  const fourxx = await query(`
    SELECT
      status_code,
      CASE
        WHEN status_code IN (401, 403) THEN 'auth'
        WHEN status_code = 404 THEN 'not found'
        ELSE 'other 4xx'
      END AS kind,
      regexp_replace(split_part(url, '?', 1),
        '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', ':id', 'g') AS route,
      count(*) AS cnt
    FROM logs
    WHERE status_code BETWEEN 400 AND 499
      AND url NOT LIKE '%/healthcheck%'
    GROUP BY status_code, kind, route
    ORDER BY cnt DESC
    LIMIT 20
  `);
  lines.push(heading(3, '4xx Breakdown'));
  lines.push(
    table(
      ['Status', 'Kind', 'Route', 'Count'],
      fourxx.map((r) => [r.status_code as number, r.kind as string, r.route as string, Number(r.cnt).toLocaleString()])
    )
  );

  const fivexx = await query(`
    SELECT
      status_code,
      regexp_replace(split_part(url, '?', 1),
        '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', ':id', 'g') AS route,
      count(*) AS cnt
    FROM logs
    WHERE status_code BETWEEN 500 AND 599
      AND url NOT LIKE '%/healthcheck%'
    GROUP BY status_code, route
    ORDER BY cnt DESC
    LIMIT 20
  `);
  lines.push(heading(3, '5xx Breakdown'));
  lines.push(
    table(
      ['Status', 'Route', 'Count'],
      fivexx.map((r) => [r.status_code as number, r.route as string, Number(r.cnt).toLocaleString()])
    )
  );

  return lines.join('\n');
}
