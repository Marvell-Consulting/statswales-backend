import { query } from '../duckdb-helper';
import { heading, table } from '../markdown';

export async function timeOfDay(): Promise<string> {
  const lines: string[] = [heading(2, 'Time-of-Day Patterns')];

  // Requests per hour of day
  const hourly = await query(`
    SELECT
      extract(hour FROM to_timestamp(log_time / 1000)) AS hour_utc,
      count(*) AS total,
      count(*) FILTER (WHERE level >= 50) AS errors
    FROM logs
    WHERE url NOT LIKE '%/healthcheck%' OR url IS NULL
    GROUP BY hour_utc
    ORDER BY hour_utc
  `);
  lines.push(heading(3, 'Requests by Hour of Day (UTC, excl. healthchecks)'));
  lines.push(
    table(
      ['Hour (UTC)', 'Requests', 'Errors', 'Error %'],
      hourly.map((r) => {
        const total = Number(r.total);
        const errs = Number(r.errors);
        const pct = total > 0 ? ((errs / total) * 100).toFixed(2) : '0.00';
        return [`${String(r.hour_utc).padStart(2, '0')}:00`, total.toLocaleString(), errs.toLocaleString(), `${pct}%`];
      })
    )
  );

  // Weekday vs weekend
  const dayOfWeek = await query(`
    SELECT
      dayname(to_timestamp(log_time / 1000)) AS day_name,
      extract(dow FROM to_timestamp(log_time / 1000)) AS dow,
      CASE
        WHEN extract(dow FROM to_timestamp(log_time / 1000)) IN (0, 6) THEN 'weekend'
        ELSE 'weekday'
      END AS period,
      count(*) AS cnt
    FROM logs
    WHERE url NOT LIKE '%/healthcheck%' OR url IS NULL
    GROUP BY day_name, dow, period
    ORDER BY dow
  `);
  lines.push(heading(3, 'Requests by Day of Week'));
  lines.push(
    table(
      ['Day', 'Period', 'Requests'],
      dayOfWeek.map((r) => [r.day_name as string, r.period as string, Number(r.cnt).toLocaleString()])
    )
  );

  // Off-hours traffic (00:00-06:00 UTC, roughly midnight-6am UK time)
  const offHours = await query(`
    SELECT
      method,
      regexp_replace(split_part(url, '?', 1),
        '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', ':id', 'g') AS route,
      count(*) AS cnt
    FROM logs
    WHERE extract(hour FROM to_timestamp(log_time / 1000)) BETWEEN 0 AND 5
      AND url IS NOT NULL
      AND url NOT LIKE '%/healthcheck%'
    GROUP BY method, route
    ORDER BY cnt DESC
    LIMIT 15
  `);
  lines.push(heading(3, 'Off-Hours Traffic (00:00-06:00 UTC)'));
  if (offHours.length === 0) {
    lines.push('_No off-hours traffic detected._\n');
  } else {
    lines.push(
      table(
        ['Method', 'Route', 'Count'],
        offHours.map((r) => [r.method as string, r.route as string, Number(r.cnt).toLocaleString()])
      )
    );
  }

  return lines.join('\n');
}
