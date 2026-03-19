import { query } from '../duckdb-helper';
import { heading, table, bold } from '../markdown';

export async function overview(): Promise<string> {
  const lines: string[] = [heading(2, 'Overview')];

  const [timeRange] = await query(`
    SELECT
      min(log_time) AS earliest,
      max(log_time) AS latest,
      count(*) AS total_rows
    FROM logs
  `);
  const earliest = new Date(Number(timeRange.earliest)).toISOString();
  const latest = new Date(Number(timeRange.latest)).toISOString();
  lines.push(`${bold('Time range:')} ${earliest} → ${latest}`);
  lines.push(`${bold('Total rows:')} ${Number(timeRange.total_rows).toLocaleString()}\n`);

  const healthcheck = await query(`
    SELECT
      count(*) FILTER (WHERE url LIKE '%/healthcheck%') AS healthcheck,
      count(*) FILTER (WHERE url NOT LIKE '%/healthcheck%' OR url IS NULL) AS non_healthcheck
    FROM logs
  `);
  lines.push(`${bold('Healthcheck requests:')} ${Number(healthcheck[0].healthcheck).toLocaleString()}`);
  lines.push(`${bold('Non-healthcheck requests:')} ${Number(healthcheck[0].non_healthcheck).toLocaleString()}\n`);

  const levels = await query(`
    SELECT
      CASE level
        WHEN 10 THEN 'trace'
        WHEN 20 THEN 'debug'
        WHEN 30 THEN 'info'
        WHEN 40 THEN 'warn'
        WHEN 50 THEN 'error'
        WHEN 60 THEN 'fatal'
        ELSE 'unknown (' || COALESCE(CAST(level AS VARCHAR), 'null') || ')'
      END AS level_name,
      count(*) AS cnt
    FROM logs
    GROUP BY level
    ORDER BY level
  `);
  lines.push(heading(3, 'Log Level Distribution'));
  lines.push(
    table(
      ['Level', 'Count'],
      levels.map((r) => [r.level_name as string, Number(r.cnt).toLocaleString()])
    )
  );

  const busiest = await query(`
    SELECT
      strftime(to_timestamp(log_time / 1000), '%Y-%m-%d %H:00') AS hour,
      count(*) AS cnt
    FROM logs
    WHERE url NOT LIKE '%/healthcheck%' OR url IS NULL
    GROUP BY hour
    ORDER BY cnt DESC
    LIMIT 10
  `);
  lines.push(heading(3, 'Top 10 Busiest Hours (excl. healthchecks)'));
  lines.push(
    table(
      ['Hour', 'Requests'],
      busiest.map((r) => [r.hour as string, Number(r.cnt).toLocaleString()])
    )
  );

  const hosts = await query(`
    SELECT hostname, count(*) AS cnt
    FROM logs
    WHERE hostname IS NOT NULL
    GROUP BY hostname
    ORDER BY cnt DESC
  `);
  lines.push(heading(3, 'Container Replicas'));
  lines.push(`${bold('Unique hostnames:')} ${hosts.length}\n`);
  lines.push(
    table(
      ['Hostname', 'Log Count'],
      hosts.map((r) => [r.hostname as string, Number(r.cnt).toLocaleString()])
    )
  );

  return lines.join('\n');
}
