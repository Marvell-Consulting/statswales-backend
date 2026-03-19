import { query } from '../duckdb-helper';
import { heading, table, bold } from '../markdown';

export async function coverageGaps(): Promise<string> {
  const lines: string[] = [heading(2, 'Coverage Gaps')];

  // Errors missing stack or type
  const missingInfo = await query(`
    SELECT
      count(*) FILTER (WHERE err_stack IS NULL AND level >= 50) AS errors_no_stack,
      count(*) FILTER (WHERE err_type IS NULL AND level >= 50) AS errors_no_type,
      count(*) FILTER (WHERE level >= 50) AS total_errors
    FROM logs
  `);
  const row = missingInfo[0];
  lines.push(`${bold('Total errors:')} ${Number(row.total_errors).toLocaleString()}`);
  lines.push(`${bold('Errors missing stack trace:')} ${Number(row.errors_no_stack).toLocaleString()}`);
  lines.push(`${bold('Errors missing error type:')} ${Number(row.errors_no_type).toLocaleString()}\n`);

  // Non-HTTP log entries
  const nonHttp = await query(`
    SELECT
      COALESCE(msg, 'no message') AS message,
      count(*) AS cnt
    FROM logs
    WHERE method IS NULL AND url IS NULL
    GROUP BY message
    ORDER BY cnt DESC
    LIMIT 20
  `);
  lines.push(heading(3, 'Non-HTTP Log Entries (no req field)'));
  lines.push(
    table(
      ['Message', 'Count'],
      nonHttp.map((r) => [(r.message as string).substring(0, 120), Number(r.cnt).toLocaleString()])
    )
  );

  // Warn messages
  const warns = await query(`
    SELECT
      COALESCE(msg, 'no message') AS message,
      COALESCE(route, 'n/a') AS route,
      count(*) AS cnt
    FROM logs
    WHERE level = 40
    GROUP BY message, route
    ORDER BY cnt DESC
    LIMIT 20
  `);
  lines.push(heading(3, 'Warn-Level Messages'));
  lines.push(
    table(
      ['Message', 'Route', 'Count'],
      warns.map((r) => [(r.message as string).substring(0, 80), r.route as string, Number(r.cnt).toLocaleString()])
    )
  );

  // Routes that never error
  const noErrors = await query(`
    WITH all_routes AS (
      SELECT DISTINCT route
      FROM logs
      WHERE route IS NOT NULL AND url NOT LIKE '%/healthcheck%'
    ),
    error_routes AS (
      SELECT DISTINCT route
      FROM logs
      WHERE level >= 50 AND route IS NOT NULL
    )
    SELECT ar.route, count(*) AS request_count
    FROM all_routes ar
    LEFT JOIN error_routes er ON ar.route = er.route
    INNER JOIN logs l ON l.route = ar.route
    WHERE er.route IS NULL
    GROUP BY ar.route
    ORDER BY request_count DESC
    LIMIT 20
  `);
  lines.push(heading(3, 'Routes That Never Produce Errors'));
  lines.push(
    table(
      ['Route', 'Request Count'],
      noErrors.map((r) => [r.route as string, Number(r.request_count).toLocaleString()])
    )
  );

  return lines.join('\n');
}
