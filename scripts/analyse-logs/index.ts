import path from 'node:path';

import { initDuckDB, run, closeDuckDB } from './duckdb-helper';
import { heading } from './markdown';
import { overview } from './sections/overview';
import { errors } from './sections/errors';
import { performance } from './sections/performance';
import { statusCodes } from './sections/status-codes';
import { abuseDetection } from './sections/abuse-detection';
import { coverageGaps } from './sections/coverage-gaps';
import { timeOfDay } from './sections/time-of-day';

async function createBaseView(csvPath: string): Promise<void> {
  const absolutePath = path.resolve(csvPath).replace(/'/g, "''");
  await run(`
    CREATE OR REPLACE TEMP TABLE logs AS
    SELECT
      json_extract_string("Log_s", '$.level')::INTEGER AS level,
      json_extract_string("Log_s", '$.time')::BIGINT AS log_time,
      json_extract_string("Log_s", '$.msg') AS msg,
      json_extract_string("Log_s", '$.req.method') AS method,
      json_extract_string("Log_s", '$.req.url') AS url,
      json_extract_string("Log_s", '$.res.statusCode')::INTEGER AS status_code,
      json_extract_string("Log_s", '$.responseTime')::DOUBLE AS response_time,
      json_extract_string("Log_s", '$.hostname') AS hostname,
      json_extract_string("Log_s", '$.err.type') AS err_type,
      json_extract_string("Log_s", '$.err.message') AS err_message,
      json_extract_string("Log_s", '$.err.stack') AS err_stack,
      json_extract("Log_s", '$.req.query') AS query_params,
      regexp_replace(split_part(json_extract_string("Log_s", '$.req.url'), '?', 1),
        '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', ':id', 'g') AS route
    FROM read_csv('${absolutePath}', header=true, ignore_errors=true)
    WHERE "Log_s" IS NOT NULL AND left(trim("Log_s"), 1) = '{'
  `);
}

async function main(): Promise<void> {
  const csvPath = process.argv[2];
  if (!csvPath) {
    process.stderr.write('Usage: npm run analyse-logs -- <path-to-csv>\n');
    process.exitCode = 1;
    return;
  }

  await initDuckDB();
  try {
    await createBaseView(csvPath);

    const sections = [
      heading(1, 'Production Log Analysis Report'),
      `_Generated: ${new Date().toISOString()}_\n`,
      await overview(),
      await errors(),
      await performance(),
      await statusCodes(),
      await abuseDetection(),
      await coverageGaps(),
      await timeOfDay()
    ];

    process.stdout.write(sections.join('\n'));
  } finally {
    await closeDuckDB();
  }
}

main().catch((err) => {
  process.stderr.write(`${err}\n`);
  process.exitCode = 1;
});
