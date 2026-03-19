export function heading(level: number, text: string): string {
  return `${'#'.repeat(level)} ${text}\n`;
}

export function table(headers: string[], rows: (string | number)[][]): string {
  if (rows.length === 0) return '_No data._\n';
  const sep = headers.map(() => '---');
  const lines = [
    `| ${headers.join(' | ')} |`,
    `| ${sep.join(' | ')} |`,
    ...rows.map((row) => `| ${row.map((c) => String(c ?? '')).join(' | ')} |`)
  ];
  return lines.join('\n') + '\n';
}

export function codeBlock(content: string, lang = ''): string {
  return `\`\`\`${lang}\n${content}\n\`\`\`\n`;
}

export function bold(text: string): string {
  return `**${text}**`;
}
