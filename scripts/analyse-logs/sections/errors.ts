import { query } from '../duckdb-helper';
import { heading, table, codeBlock } from '../markdown';

export async function errors(): Promise<string> {
  const lines: string[] = [heading(2, 'Errors')];

  const grouped = await query(`
    SELECT
      COALESCE(err_type, 'unknown') AS error_type,
      COALESCE(split_part(err_message, chr(10), 1), 'no message') AS first_line,
      count(*) AS cnt,
      max(to_timestamp(log_time / 1000))::VARCHAR AS most_recent
    FROM logs
    WHERE level >= 50
    GROUP BY error_type, first_line
    ORDER BY cnt DESC
    LIMIT 30
  `);
  lines.push(heading(3, 'Errors by Type + Message'));
  lines.push(
    table(
      ['Type', 'Message (first line)', 'Count', 'Most Recent'],
      grouped.map((r) => [
        r.error_type as string,
        (r.first_line as string).substring(0, 120),
        Number(r.cnt).toLocaleString(),
        r.most_recent as string
      ])
    )
  );

  const stacks = await query(`
    SELECT
      COALESCE(err_type, 'unknown') AS error_type,
      COALESCE(split_part(err_message, chr(10), 1), 'no message') AS first_line,
      err_stack
    FROM logs
    WHERE level >= 50 AND err_stack IS NOT NULL
    GROUP BY error_type, first_line, err_stack
    ORDER BY count(*) DESC
    LIMIT 5
  `);
  if (stacks.length > 0) {
    lines.push(heading(3, 'Example Stack Traces (top 5 error types)'));
    for (const s of stacks) {
      lines.push(`**${s.error_type}: ${(s.first_line as string).substring(0, 100)}**\n`);
      const stackLines = (s.err_stack as string).split('\n').slice(0, 15);
      lines.push(codeBlock(stackLines.join('\n')));
    }
  }

  const routes = await query(`
    SELECT
      regexp_replace(split_part(url, '?', 1),
        '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', ':id', 'g') AS route,
      count(*) AS cnt
    FROM logs
    WHERE level >= 50 AND url IS NOT NULL
    GROUP BY route
    ORDER BY cnt DESC
    LIMIT 15
  `);
  lines.push(heading(3, 'Top Error-Producing Routes'));
  lines.push(
    table(
      ['Route', 'Error Count'],
      routes.map((r) => [r.route as string, Number(r.cnt).toLocaleString()])
    )
  );

  const noStack = await query(`
    SELECT count(*) AS cnt FROM logs WHERE level >= 50 AND err_stack IS NULL
  `);
  lines.push(`\n**Errors without stack traces:** ${Number(noStack[0].cnt).toLocaleString()}\n`);

  return lines.join('\n');
}
