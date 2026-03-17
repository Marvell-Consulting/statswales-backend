/* eslint-disable @typescript-eslint/no-explicit-any */
export interface SchemaTranslation {
  description?: string;
  properties?: Record<string, SchemaTranslation>;
}

export interface TranslationMap {
  info?: {
    title?: string;
    description?: string;
  };
  tags?: Record<string, string>; // tag name → Welsh description
  operations?: Record<string, { summary?: string; description?: string }>; // "GET /" → translations
  responses?: Record<string, Record<string, string>>; // "GET /" → { "200": "Welsh description" }
  parameters?: Record<string, string>; // parameter name → Welsh description
  schemas?: Record<string, SchemaTranslation>; // schema name → property translations
}

function translateSchemaProperties(schema: Record<string, any>, translation: SchemaTranslation): void {
  if (translation.description) {
    schema.description = translation.description;
  }
  if (translation.properties) {
    // Collect all property bags to translate: direct, allOf items, and array items
    const propertyBags: Record<string, any>[] = [];
    if (schema.properties) propertyBags.push(schema.properties);
    if (schema.items?.properties) propertyBags.push(schema.items.properties);
    if (Array.isArray(schema.allOf)) {
      for (const item of schema.allOf) {
        if (item.properties) propertyBags.push(item.properties);
      }
    }

    for (const bag of propertyBags) {
      for (const [propName, propTranslation] of Object.entries(translation.properties)) {
        if (bag[propName]) {
          translateSchemaProperties(bag[propName], propTranslation);
        }
      }
    }
  }
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

  // Translate schema component property descriptions
  if (translations.schemas && translated.components?.schemas) {
    for (const [schemaName, schemaTranslation] of Object.entries(translations.schemas)) {
      if (translated.components.schemas[schemaName]) {
        translateSchemaProperties(translated.components.schemas[schemaName], schemaTranslation);
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
