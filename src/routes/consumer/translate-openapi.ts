/* eslint-disable @typescript-eslint/no-explicit-any */
export interface TranslationMap {
  info?: {
    title?: string;
    description?: string;
  };
  tags?: Record<string, string>; // tag name → Welsh description
  operations?: Record<string, { summary?: string; description?: string }>; // "GET /" → translations
  responses?: Record<string, Record<string, string>>; // "GET /" → { "200": "Welsh description" }
  parameters?: Record<string, string>; // parameter name → Welsh description
}

export function translateSpec(spec: Record<string, any>, translations: TranslationMap): Record<string, any> {
  const translated = JSON.parse(JSON.stringify(spec));

  // Translate info block
  if (translations.info) {
    if (translations.info.title) translated.info.title = translations.info.title;
    if (translations.info.description) translated.info.description = translations.info.description;
  }

  // Translate tag descriptions (matched by tag name)
  if (translations.tags && translated.tags) {
    for (const tag of translated.tags) {
      if (translations.tags[tag.name]) {
        tag.description = translations.tags[tag.name];
      }
    }
  }

  // Translate operation summaries, descriptions, and response descriptions
  const httpMethods = new Set(['get', 'post', 'put', 'delete', 'patch', 'options', 'head', 'trace']);
  if (translated.paths) {
    for (const [path, methods] of Object.entries<any>(translated.paths)) {
      for (const [method, operation] of Object.entries<any>(methods)) {
        if (!httpMethods.has(method.toLowerCase())) continue;
        const key = `${method.toUpperCase()} ${path}`;

        // Operation summary/description
        if (translations.operations?.[key]) {
          const t = translations.operations[key];
          if (t.summary) operation.summary = t.summary;
          if (t.description) operation.description = t.description;
        }

        // Response descriptions
        if (translations.responses?.[key] && operation.responses) {
          for (const [code, desc] of Object.entries(translations.responses[key])) {
            if (operation.responses[code]) {
              operation.responses[code].description = desc;
            }
          }
        }
      }
    }
  }

  // Translate component parameter descriptions
  if (translations.parameters && translated.components?.parameters) {
    for (const [name, desc] of Object.entries(translations.parameters)) {
      if (translated.components.parameters[name]) {
        translated.components.parameters[name].description = desc;
      }
    }
  }

  return translated;
}
