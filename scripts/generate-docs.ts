import 'dotenv/config';
import path from 'node:path';

import swaggerAutogen from 'swagger-autogen';

/**
 * This script generates the OpenAPI spec file for the StatsWales 3 Consumer API, which is then used to render the api
 * documentation via src/routes/api-doc.ts.
 *
 * This should run automatically on build, but can also be run manually with `npm run docs:generate`.
 */
const consumerEndpoints = ['./src/routes/consumer/v1/api.ts'];
const outputFile = path.join(__dirname, '../src/routes/consumer/v1/openapi.json');

const doc = {
  info: {
    version: '1.0.0',
    title: 'StatsWales 3 Consumer API',
    description: 'This website provides documentation for StatsWales 3 public API.',
    contact: {
      name: 'StatsWales Support',
      email: 'StatsWales@gov.wales'
    }
  },
  servers: [
    { description: 'Local', url: 'https://localhost:3001/v1' },
    { description: 'Production', url: 'https://api.stats.gov.wales/v1' }
  ],
  components: {
    parameters: {
      language: {
        name: 'accept-language',
        in: 'header',
        description: 'Language for the response. Supported languages: "cy" for Welsh, "en" for English',
        required: false,
        type: 'string',
        default: 'en'
      },
      page: {
        name: 'page',
        in: 'query',
        description: 'Page number for pagination',
        required: false,
        type: 'integer',
        default: 1
      },
      limit: {
        name: 'limit',
        in: 'query',
        description: 'Number of datasets per page',
        required: false,
        type: 'integer',
        default: 10
      },
      dataset_id: {
        name: 'dataset_id',
        in: 'path',
        description: 'The unique identifier of the dataset to retrieve',
        required: true,
        type: 'string',
        format: 'uuid',
        example: '141baa8a-2ed0-45cb-ad4a-83de8c2333b5'
      }
    },
    // eslint-disable-next-line @typescript-eslint/naming-convention
    '@schemas': {
      Revision: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid', description: 'Unique identifier for the revision' },
          revision_index: { type: 'integer', description: 'Version number, starting from 1' },
          dataset_id: {
            type: 'string',
            format: 'uuid',
            description: 'Unique identifier for the dataset this revision belongs to'
          },
          previous_revision_id: {
            type: 'string',
            format: 'uuid',
            description: 'Unique identifier for the previous revision, if any'
          },
          created_at: {
            type: 'string',
            format: 'date-time',
            description: 'Creation date of the revision in ISO 8601 format'
          },
          updated_at: {
            type: 'string',
            format: 'date-time',
            description: 'Last update date of the revision in ISO 8601 format'
          },
          publish_at: {
            type: 'string',
            format: 'date-time',
            description: 'Publication date of the revision in ISO 8601 format'
          },
          metadata: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                language: {
                  type: 'string',
                  description: 'Language of the metadata, e.g., "en" for English, "cy" for Welsh'
                },
                title: {
                  type: 'string',
                  description: 'Title of the revision in the specified language'
                },
                summary: {
                  type: 'string',
                  description: 'Summary of the revision in the specified language'
                },
                collection: {
                  type: 'string',
                  description: 'Collection name for the revision in the specified language'
                },
                quality: {
                  type: 'string',
                  description: 'Quality information for the revision in the specified language'
                },
                rounding_description: {
                  type: 'string',
                  description: 'Description of rounding applied to the data in this revision'
                }
              }
            },
            description: 'Array of metadata key-value pairs for the revision'
          },
          rounding_applied: {
            type: 'boolean',
            description: 'Indicates if rounding was applied to the data in this revision'
          },
          update_frequency: {
            type: 'object',
            properties: {
              is_updated: {
                type: 'boolean',
                description: 'Indicates if the dataset is updated regularly'
              },
              frequency_value: {
                type: 'integer',
                description: 'The numeric value of the update frequency'
              },
              frequency_unit: {
                type: 'string',
                description: 'The unit of the update frequency',
                enum: ['day', 'week', 'month', 'year']
              }
            }
          },
          designation: {
            type: 'string',
            description: 'The designation of the revision'
          },
          related_links: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                id: { type: 'string', description: 'Unique identifier for the related link' },
                url: { type: 'string', format: 'uri', description: 'URL of the related link' },
                label_en: { type: 'string', description: 'Label of the related link in English' },
                label_cy: { type: 'string', description: 'Label of the related link in Welsh' },
                created_at: {
                  type: 'string',
                  format: 'date-time',
                  description: 'Creation date of the related link in ISO 8601 format'
                }
              }
            }
          },
          topics: {
            type: 'array',
            items: { $ref: '#/components/schemas/Topic' }
          }
        }
      },
      Dataset: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid', description: 'Unique identifier for the dataset' },
          live: {
            type: 'string',
            format: 'date-time',
            description: 'First publication date of the dataset in ISO 8601 format'
          },
          start_date: {
            type: 'string',
            format: 'date',
            description: 'Start date of the dataset in ISO 8601 format'
          },
          end_date: {
            type: 'string',
            format: 'date',
            description: 'End date of the dataset in ISO 8601 format'
          },
          published_revision: { $ref: '#/components/schemas/Revision' },
          revisions: { type: 'array', items: { $ref: '#/components/schemas/Revision' } },
          dimensions: {}
        },
        example: {
          id: '141baa8a-2ed0-45cb-ad4a-83de8c2333b5',
          live: '2023-01-01T00:00:00Z',
          start_date: '2020-01-01',
          end_date: '2023-12-31',
          published_revision: {
            id: 'd1f2e3a4-5678-90ab-cdef-1234567890ab',
            revision_index: 5,
            dataset_id: '141baa8a-2ed0-45cb-ad4a-83de8c2333b5',
            previous_revision_id: 'c0b1a2d3-4567-89ab-cdef-1234567890ab',
            created_at: '2023-01-01T00:00:00Z',
            updated_at: '2023-01-02T00:00:00Z',
            publish_at: '2023-01-03T00:00:00Z',
            metadata: [
              { language: 'en', title: 'Population Estimates', summary: 'Annual population estimates for Wales' },
              {
                language: 'cy',
                title: 'Amcangyfrifon poblogaeth',
                summary: 'Amcangyfrifon poblogaeth flynyddol ar gyfer Cymru'
              }
            ],
            rounding_applied: false,
            update_frequency: { is_updated: true, frequency_value: 1, frequency_unit: 'year' },
            designation: 'official',
            related_links: [
              {
                id: 'ert23',
                url: 'https://example.com/population-estimates',
                label_en: 'Population Estimates Data',
                label_cy: 'Data Amcangyfrifon Poblogaeth',
                created_at: '2023-01-01T00:00:00Z'
              }
            ],
            topics: [{ id: 1, path: '1', name: 'Population', name_en: 'Population', name_cy: 'Poblogaeth' }]
          }
        }
      },
      DatasetListItem: {
        type: 'object',
        properties: {
          id: {
            type: 'string',
            format: 'uuid',
            description: 'Unique identifier for the dataset'
          },
          title: {
            type: 'string',
            description: 'Title of the dataset (in the language requested via accept-language header)'
          },
          published_date: {
            type: 'string',
            format: 'date-time',
            description: 'First publication date of the dataset in ISO 8601 format'
          }
        }
      },
      DatasetsWithCount: {
        type: 'object',
        properties: {
          data: { type: 'array', items: { $ref: '#/components/schemas/DatasetListItem' } },
          count: { type: 'integer', description: 'Total number of datasets' }
        },
        example: {
          data: [
            {
              id: '141baa8a-2ed0-45cb-ad4a-83de8c2333b5',
              title: 'Population Estimates',
              published_date: '2023-01-01T00:00:00Z'
            },
            {
              id: '0ff18b56-0a4f-4ac3-a198-197aa48cc9e1',
              title: 'Economic Indicators',
              published_date: '2023-02-01T00:00:00Z'
            }
          ],
          count: 57
        }
      },
      Topic: {
        type: 'object',
        properties: {
          id: { type: 'integer', description: 'ID of the topic', example: 92 },
          path: { type: 'string', description: 'Path of the topic', example: '92' },
          name: { type: 'string', description: 'Name in the current language', example: 'Transport' },
          name_en: { type: 'string', description: 'Name of the topic in English', example: 'Transport' },
          name_cy: { type: 'string', description: 'Name of the topic in Welsh', example: 'Trafnidiaeth' }
        }
      },
      RootTopics: {
        type: 'object',
        properties: {
          children: { type: 'array', items: { $ref: '#/components/schemas/Topic' } }
        },
        example: {
          children: [
            {
              id: 1,
              path: '1',
              name: "Busnes, economi a'r farchnad lafur",
              name_en: 'Business, economy and labour market',
              name_cy: "Busnes, economi a'r farchnad lafur"
            },
            {
              id: 13,
              path: '13',
              name: 'Addysg a hyfforddiant',
              name_en: 'Education and training',
              name_cy: 'Addysg a hyfforddiant'
            },
            {
              id: 23,
              path: '23',
              name: 'Amgylchedd, ynni ac amaethyddiaeth',
              name_en: 'Environment, energy and agriculture',
              name_cy: 'Amgylchedd, ynni ac amaethyddiaeth'
            }
          ]
        }
      },
      PublishedTopics: {
        type: 'object',
        properties: {
          selectedTopic: { $ref: '#/components/schemas/Topic' },
          children: { type: 'array', items: { $ref: '#/components/schemas/Topic' } },
          parents: { type: 'array', items: { $ref: '#/components/schemas/Topic' } },
          datasets: { $ref: '#/components/schemas/DatasetsWithCount' }
        }
      }
    }
  }
};

const generateDocs = swaggerAutogen({ openapi: '3.1.1', language: 'en-GB' });

generateDocs(outputFile, consumerEndpoints, doc);
