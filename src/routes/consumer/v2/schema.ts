/* eslint-disable @typescript-eslint/naming-convention */

import { DownloadFormat } from '../../../enums/download-format';
import { DataValueType } from '../../../enums/data-value-type';
import { DEFAULT_PAGE_SIZE } from '../../../utils/page-defaults';

export const schemaV2 = {
  info: {
    version: '2.0.0',
    title: 'StatsWales public API',
    description: `This page will help you use the public API for StatsWales. If you need any other support,
      <a href="mailto:StatsWales@gov.wales">contact StatsWales</a>.`
  },
  servers: [{ description: 'Public API', url: '{{backendURL}}/v2' }],
  tags: [
    { name: 'Datasets', description: 'Browse, search, and retrieve metadata for published datasets.' },
    { name: 'Topics', description: 'Navigate the topic hierarchy used to categorise datasets.' },
    {
      name: 'Data',
      description: 'Retrieve paginated tabular data for a dataset, with optional filtering and sorting.'
    },
    { name: 'Pivot', description: 'Retrieve a cross-tabulated (pivot table) view of dataset data.' },
    {
      name: 'Query',
      description: 'Inspect stored query configurations, including filter options, row counts, and column mappings.'
    }
  ],
  components: {
    parameters: {
      language: {
        name: 'lang',
        in: 'query',
        description: 'Language to use for the response, "cy" or "cy-gb" for Welsh and "en" or "en-gb" for English',
        required: false,
        schema: { type: 'string', enum: ['cy', 'en', 'cy-gb', 'en-gb'], default: 'en-gb' }
      },
      dataset_id: {
        name: 'dataset_id',
        in: 'path',
        description: 'The unique identifier of the desired dataset',
        required: true,
        schema: { type: 'string', format: 'uuid' },
        example: '141baa8a-2ed0-45cb-ad4a-83de8c2333b5'
      },
      topic_id: {
        name: 'topic_id',
        in: 'path',
        description: 'The unique identifier of the desired topic',
        required: true,
        schema: { type: 'integer' },
        example: '91'
      },
      format: {
        name: 'format',
        in: 'path',
        description: 'File format for the download',
        required: true,
        schema: { type: 'string', enum: Object.values(DownloadFormat) },
        example: 'csv'
      },
      page_number: {
        name: 'page_number',
        in: 'query',
        description: 'Page number for pagination',
        required: false,
        schema: { type: 'integer', default: 1 }
      },
      page_size: {
        name: 'page_size',
        in: 'query',
        description: 'Number of datasets per page',
        required: false,
        schema: { type: 'integer', default: DEFAULT_PAGE_SIZE }
      },
      sort_by: {
        name: 'sort_by',
        in: 'query',
        description:
          'How to sort the data. You need to include the `columnName` and whether the column is ascending or descending (`asc` or `desc`). Direction is ascending by default. See example for how to format this.',
        required: false,
        schema: { type: 'string' },
        example: 'title:asc,last_updated_at:desc'
      },
      filter: {
        name: 'filter',
        in: 'query',
        description: `Properties to filter the data by. The value should be a JSON array of objects sent as a URL
        encoded string.`,
        required: false,
        schema: {
          type: 'string',
          description: 'JSON string containing an array of filter objects'
        },
        example: `[{"columnName": "Area", "values": ["England","Wales"]}, {"columnName": "Year", "values": ["2020"]}]`
      },
      keywords: {
        name: 'keywords',
        in: 'query',
        description: 'Search query string',
        required: true,
        schema: { type: 'string' }
      },
      revision_id: {
        name: 'revision_id',
        in: 'path',
        description: 'The unique identifier of the revision',
        required: true,
        schema: { type: 'string', format: 'uuid' }
      },
      filter_id: {
        name: 'filter_id',
        in: 'path',
        description: 'Filter ID returned by the POST /data or POST /pivot endpoint',
        required: true,
        schema: { type: 'string' }
      },
      search_mode: {
        name: 'mode',
        in: 'query',
        description:
          "Search algorithm to use. **basic** (default): case-insensitive substring match against title and summary. **basic_split**: splits keywords into individual words and requires all of them to appear (AND logic). **fts**: PostgreSQL full-text search using language-aware stemming and ranking — returns `rank`, `match_title`, and `match_summary` fields with highlighted matches. **fts_simple**: like fts but uses the 'simple' dictionary (no stemming), useful for Welsh-language searches. **fuzzy**: trigram-based similarity matching — tolerant of typos and partial matches.",
        required: false,
        schema: {
          type: 'string',
          enum: ['basic', 'basic_split', 'fts', 'fts_simple', 'fuzzy'],
          default: 'basic'
        }
      }
    },
    '@schemas': {
      RevisionMetadata: {
        type: 'object',
        properties: {
          language: {
            type: 'string',
            description: 'Language code of the metadata, e.g. "en-GB" or "cy-GB"'
          },
          title: { type: 'string', description: 'Title of the revision in the specified language' },
          summary: { type: 'string', description: 'Summary of the revision in the specified language' },
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
          },
          reason: {
            type: 'string',
            description: 'Reason for the update, present on revisions after the first'
          }
        }
      },
      UpdateFrequency: {
        type: 'object',
        properties: {
          update_type: {
            type: 'string',
            enum: ['update', 'replacement', 'none'],
            description:
              'Type of update: "update" for new data added, "replacement" for full replacement, "none" for no further updates'
          },
          date: {
            type: 'object',
            description: 'Next expected update date',
            properties: {
              day: { type: 'string', description: 'Day of the month' },
              month: { type: 'string', description: 'Month number' },
              year: { type: 'string', description: 'Four-digit year' }
            }
          }
        }
      },
      RelatedLink: {
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
      },
      Provider: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid' },
          group_id: { type: 'string', format: 'uuid' },
          revision_id: { type: 'string', format: 'uuid' },
          language: { type: 'string', description: 'Language code, e.g. "en-gb" or "cy-gb"' },
          provider_id: { type: 'string', format: 'uuid' },
          provider_name: { type: 'string', description: 'Name of the data provider' },
          source_id: { type: 'string', format: 'uuid' },
          source_name: { type: 'string', description: 'Name of the data source' },
          created_at: { type: 'string', format: 'date-time' }
        }
      },
      Revision: {
        type: 'object',
        description: 'Revision as returned within a dataset response (all languages included in metadata).',
        properties: {
          id: { type: 'string', format: 'uuid', description: 'Unique identifier for the revision' },
          revision_index: { type: 'integer', description: 'Version number, starting from 1' },
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
          approved_at: {
            type: 'string',
            format: 'date-time',
            description: 'Approval date of the revision in ISO 8601 format'
          },
          publish_at: {
            type: 'string',
            format: 'date-time',
            description: 'Publication date of the revision in ISO 8601 format'
          },
          unpublished_at: {
            type: 'string',
            format: 'date-time',
            description: 'Date the revision was unpublished, if applicable'
          },
          coverage_start_date: {
            type: 'string',
            format: 'date-time',
            description:
              'Start of the time period covered by the data in this revision, in ISO 8601 format. Only present for datasets with date-type dimensions.'
          },
          coverage_end_date: {
            type: 'string',
            format: 'date-time',
            description:
              'End of the time period covered by the data in this revision, in ISO 8601 format. Only present for datasets with date-type dimensions.'
          },
          metadata: {
            type: 'array',
            items: { $ref: '#/components/schemas/RevisionMetadata' },
            description: 'Metadata for each language (typically en-GB and cy-GB)'
          },
          rounding_applied: {
            type: 'boolean',
            description: 'Indicates if rounding was applied to the data in this revision'
          },
          update_frequency: { $ref: '#/components/schemas/UpdateFrequency' },
          designation: {
            type: 'string',
            description: 'Statistical designation of the revision',
            enum: ['official', 'accredited', 'in_development', 'management', 'none']
          },
          related_links: {
            type: 'array',
            items: { $ref: '#/components/schemas/RelatedLink' }
          },
          providers: {
            type: 'array',
            items: { $ref: '#/components/schemas/Provider' },
            description: 'Data providers and sources for this revision'
          },
          topics: {
            type: 'array',
            items: { $ref: '#/components/schemas/Topic' }
          }
        }
      },
      SingleLanguageRevision: {
        type: 'object',
        description:
          'Revision as returned by the /revision/:revision_id endpoint — metadata is a single object filtered to the requested language.',
        properties: {
          id: { type: 'string', format: 'uuid', description: 'Unique identifier for the revision' },
          revision_index: { type: 'integer', description: 'Version number, starting from 1' },
          previous_revision_id: {
            type: 'string',
            format: 'uuid',
            description: 'Unique identifier for the previous revision, if any'
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
          coverage_start_date: {
            type: 'string',
            format: 'date-time',
            description: 'Start of the time period covered by the data'
          },
          coverage_end_date: {
            type: 'string',
            format: 'date-time',
            description: 'End of the time period covered by the data'
          },
          metadata: {
            $ref: '#/components/schemas/RevisionMetadata',
            description: 'Metadata filtered to the requested language'
          },
          rounding_applied: {
            type: 'boolean',
            description: 'Indicates if rounding was applied to the data in this revision'
          },
          update_frequency: { $ref: '#/components/schemas/UpdateFrequency' },
          designation: {
            type: 'string',
            description: 'Statistical designation of the revision',
            enum: ['official', 'accredited', 'in_development', 'management', 'none']
          },
          related_links: {
            type: 'array',
            items: { $ref: '#/components/schemas/RelatedLink' }
          },
          providers: {
            type: 'array',
            items: { $ref: '#/components/schemas/Provider' },
            description: 'Data providers and sources, filtered to the requested language'
          },
          topics: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                id: { type: 'integer', description: 'ID of the topic' },
                path: { type: 'string', description: 'Path of the topic' },
                name: { type: 'string', description: 'Name of the topic in the requested language' }
              }
            }
          }
        }
      },
      Publisher: {
        type: 'object',
        properties: {
          group: {
            type: 'object',
            properties: {
              id: { type: 'string', format: 'uuid' },
              name: { type: 'string', description: 'Name of the publishing group' },
              email: { type: 'string', description: 'Contact email for the publishing group' }
            }
          },
          organisation: {
            type: 'object',
            properties: {
              id: { type: 'string', format: 'uuid' },
              name: { type: 'string', description: 'Name of the publishing organisation' }
            }
          }
        }
      },
      Dataset: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid', description: 'Unique identifier for the dataset' },
          first_published_at: {
            type: 'string',
            format: 'date-time',
            description: 'First publication date of the dataset in ISO 8601 format'
          },
          archived_at: {
            type: 'string',
            format: 'date-time',
            description: 'Date the dataset was archived in ISO 8601 format, if applicable'
          },
          published_revision: { $ref: '#/components/schemas/Revision' },
          publisher: { $ref: '#/components/schemas/Publisher' }
        },
        example: {
          id: '141baa8a-2ed0-45cb-ad4a-83de8c2333b5',
          first_published_at: '2023-01-01T00:00:00Z',
          published_revision: {
            id: 'd1f2e3a4-5678-90ab-cdef-1234567890ab',
            revision_index: 5,
            previous_revision_id: 'c0b1a2d3-4567-89ab-cdef-1234567890ab',
            created_at: '2023-01-01T00:00:00Z',
            updated_at: '2023-01-02T00:00:00Z',
            approved_at: '2023-01-02T00:00:00Z',
            publish_at: '2023-01-03T00:00:00Z',
            metadata: [
              {
                language: 'en-GB',
                title: 'Population Estimates',
                summary: 'Annual population estimates for Wales',
                collection: 'Population',
                quality: 'National Statistics',
                rounding_description: ''
              },
              {
                language: 'cy-GB',
                title: 'Amcangyfrifon poblogaeth',
                summary: 'Amcangyfrifon poblogaeth flynyddol ar gyfer Cymru',
                collection: 'Poblogaeth',
                quality: 'Ystadegau Gwladol',
                rounding_description: ''
              }
            ],
            rounding_applied: false,
            update_frequency: { update_type: 'update', date: { day: '15', month: '06', year: '2025' } },
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
            providers: [
              {
                id: '95a3acb0-2f60-4ded-8b63-3df7a9d3d2dd',
                group_id: 'a8bcf16d-99e1-45f5-bba1-a4f6978b85cd',
                revision_id: 'd1f2e3a4-5678-90ab-cdef-1234567890ab',
                language: 'en-gb',
                provider_id: '98aed6ef-122c-430b-988c-92258cb372f5',
                provider_name: 'Welsh Government',
                source_id: 'c571cc07-ba84-48ee-be47-645a7711a905',
                source_name: 'National Survey for Wales',
                created_at: '2023-01-01T00:00:00Z'
              }
            ],
            topics: [{ id: 1, path: '1', name_en: 'Population', name_cy: 'Poblogaeth' }]
          },
          publisher: {
            group: { id: 'b080588c-86b0-46e1-87be-10776bc43743', name: 'Statistics team', email: 'stats@gov.wales' },
            organisation: { id: '4ef4facf-c488-4837-a65b-e66d4b525965', name: 'Welsh Government' }
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
            description: 'Title of the dataset in the requested language'
          },
          first_published_at: {
            type: 'string',
            format: 'date-time',
            description: 'First publication date of the dataset in ISO 8601 format'
          },
          last_updated_at: {
            type: 'string',
            format: 'date-time',
            description: 'Date of the most recent update to the dataset in ISO 8601 format'
          },
          archived_at: {
            type: 'string',
            format: 'date-time',
            description: 'Date the dataset was archived in ISO 8601 format, if applicable'
          }
        }
      },
      SearchResultItem: {
        type: 'object',
        description: 'A dataset search result. Extends DatasetListItem with search-specific fields.',
        properties: {
          id: { type: 'string', format: 'uuid', description: 'Unique identifier for the dataset' },
          title: { type: 'string', description: 'Title of the dataset' },
          summary: { type: 'string', description: 'Summary of the dataset' },
          first_published_at: { type: 'string', format: 'date-time' },
          last_updated_at: { type: 'string', format: 'date-time' },
          archived_at: { type: 'string', format: 'date-time' },
          rank: {
            type: 'number',
            description: 'Relevance score (present for fts and fts_simple modes)'
          },
          match_title: {
            type: 'string',
            description:
              'Title with search term highlights wrapped in <mark> tags (present for fts and fts_simple modes)'
          },
          match_summary: {
            type: 'string',
            description:
              'Summary with search term highlights wrapped in <mark> tags (present for fts and fts_simple modes)'
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
              first_published_at: '2023-01-01T00:00:00Z',
              last_updated_at: '2023-06-15T00:00:00Z',
              archived_at: null
            },
            {
              id: '0ff18b56-0a4f-4ac3-a198-197aa48cc9e1',
              title: 'Economic Indicators',
              first_published_at: '2023-02-01T00:00:00Z',
              last_updated_at: '2023-07-01T00:00:00Z',
              archived_at: null
            }
          ],
          count: 57
        }
      },
      SearchResultsWithCount: {
        type: 'object',
        properties: {
          data: { type: 'array', items: { $ref: '#/components/schemas/SearchResultItem' } },
          count: { type: 'integer', description: 'Total number of matching datasets' }
        }
      },
      DataRow: {
        type: 'object',
        description:
          'A single data row as a JSON object. Keys are column names (fact-table names by default) and values are data values.',
        additionalProperties: { type: 'string' }
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
      SubTopic: {
        type: 'object',
        properties: {
          id: { type: 'integer', description: 'ID of the topic', example: 93 },
          path: { type: 'string', description: 'Path of the topic', example: '92.93' },
          name: { type: 'string', description: 'Name in the current language', example: 'Air' },
          name_en: { type: 'string', description: 'Name of the topic in English', example: 'Air' },
          name_cy: { type: 'string', description: 'Name of the topic in Welsh', example: 'Awyr' }
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
              name: 'Business, economy and labour market',
              name_en: 'Business, economy and labour market',
              name_cy: "Busnes, economi a'r farchnad lafur"
            },
            {
              id: 13,
              path: '13',
              name: 'Education and training',
              name_en: 'Education and training',
              name_cy: 'Addysg a hyfforddiant'
            }
          ]
        }
      },
      PublishedTopics: {
        type: 'object',
        properties: {
          selectedTopic: { $ref: '#/components/schemas/SubTopic' },
          children: {
            type: 'array',
            items: { $ref: '#/components/schemas/Topic' },
            description:
              'Sub-topics under the selected topic. Empty array if this is a leaf topic (in which case datasets will be populated).'
          },
          parents: {
            type: 'array',
            items: { $ref: '#/components/schemas/Topic' },
            description: 'Ancestor topics from root to the selected topic'
          },
          datasets: {
            $ref: '#/components/schemas/DatasetsWithCount',
            description: 'Datasets tagged to this topic. Only present for leaf topics (topics with no children).'
          }
        }
      },
      FilterValue: {
        type: 'object',
        properties: {
          reference: { type: 'string', description: 'Reference code to use in filter values' },
          description: { type: 'string', description: 'Human-readable label (language-dependent)' },
          children: {
            type: 'array',
            items: { $ref: '#/components/schemas/FilterValue' },
            description: 'Child values for hierarchical dimensions (e.g. Wales → local authorities)'
          }
        }
      },
      Filter: {
        type: 'object',
        description: 'A single filterable dimension and its allowed values.',
        properties: {
          factTableColumn: { type: 'string', description: 'Internal fact-table column name, e.g. AreaCode' },
          columnName: {
            type: 'string',
            description: 'Human-readable dimension name — use this as the key in your filter object'
          },
          values: {
            type: 'array',
            items: { $ref: '#/components/schemas/FilterValue' }
          }
        }
      },
      FilterId: {
        type: 'object',
        description:
          'A reusable, shareable identifier for a stored set of filters and display options. The same filter inputs always produce the same ID.',
        properties: {
          filterId: {
            type: 'string',
            description:
              '12-character identifier for the stored query. Pass this to GET /{dataset_id}/data/{filter_id} or GET /{dataset_id}/pivot/{filter_id} to retrieve filtered results.'
          }
        },
        example: { filterId: 'a1b2c3d4e5f6' }
      },
      DataOptions: {
        type: 'object',
        description:
          'Row filters and display options to store as a reusable query. Use column names and reference codes from GET /{dataset_id}/filters.',
        properties: {
          filters: {
            type: 'array',
            description:
              'Each object has a single key (a column name from GET /filters) mapped to an array of reference codes. Multiple objects combine with AND logic; multiple values within one object combine with OR logic.',
            items: {
              type: 'object',
              additionalProperties: {
                type: 'array',
                items: { type: 'string' }
              }
            },
            example: [{ Year: ['2020', '2021'] }, { Area: ['W92000004'] }]
          },
          options: {
            type: 'object',
            description:
              'Display options. If omitted, defaults to use_raw_column_names: true, use_reference_values: true, data_value_type: raw.',
            properties: {
              use_raw_column_names: {
                type: 'boolean',
                description:
                  'When true (default), column headers use internal fact-table names (e.g. AreaCode). When false, headers use human-readable dimension names (e.g. Area).',
                default: true
              },
              use_reference_values: {
                type: 'boolean',
                description:
                  'When true (default), cell values are reference codes (e.g. K02000001). When false, values are human-readable descriptions (e.g. United Kingdom).',
                default: true
              },
              data_value_type: {
                type: 'string',
                description:
                  'Selects the cube view used for data output. raw (default): raw data values and dates. raw_extended: raw values plus reference codes, hierarchies, and sort orders. formatted: formatted data values, no dates. formatted_extended: formatted values and dates plus reference codes, hierarchies, and sort orders. with_note_codes: data values annotated with note markers.',
                enum: Object.values(DataValueType),
                default: DataValueType.Raw
              }
            }
          }
        }
      },
      PivotOptions: {
        allOf: [
          { $ref: '#/components/schemas/DataOptions' },
          {
            type: 'object',
            required: ['pivot'],
            properties: {
              pivot: {
                type: 'object',
                required: ['x', 'y'],
                properties: {
                  x: { type: 'string', description: 'Column name for the horizontal axis of the pivot table' },
                  y: { type: 'string', description: 'Column name for the vertical axis of the pivot table' },
                  backend: {
                    type: 'string',
                    enum: ['postgres', 'duckdb'],
                    description: 'Backend engine to use for the pivot (default: duckdb)'
                  },
                  include_performance: {
                    type: 'boolean',
                    description: 'Include performance metadata in the response',
                    default: false
                  }
                }
              }
            }
          }
        ]
      },
      QueryStore: {
        type: 'object',
        properties: {
          id: { type: 'string', description: '12-character identifier for the stored query' },
          hash: { type: 'string', description: 'Hash of the query parameters for deduplication' },
          datasetId: { type: 'string', format: 'uuid', description: 'Dataset this query belongs to' },
          revisionId: { type: 'string', format: 'uuid', description: 'Revision this query belongs to' },
          requestObject: { $ref: '#/components/schemas/DataOptions' },
          query: {
            type: 'object',
            additionalProperties: { type: 'string' },
            description: 'Key-value map of language code to SQL query string'
          },
          totalLines: { type: 'integer', description: 'Total number of rows matching the query' },
          columnMapping: {
            type: 'array',
            description: 'Mapping of fact-table column names to dimension display names',
            items: {
              type: 'object',
              properties: {
                fact_table_column: { type: 'string' },
                dimension_name: { type: 'string' },
                language: { type: 'string' }
              }
            }
          },
          createdAt: { type: 'string', format: 'date-time' },
          updatedAt: { type: 'string', format: 'date-time' }
        }
      },
      Filters: {
        type: 'array',
        items: { $ref: '#/components/schemas/Filter' },
        example: [
          {
            factTableColumn: 'YearCode',
            columnName: 'Year',
            values: [
              { reference: '2020', description: '2020' },
              { reference: '2021', description: '2021' },
              { reference: '2022', description: '2022' },
              { reference: '2023', description: '2023' }
            ]
          },
          {
            factTableColumn: 'AreaCode',
            columnName: 'Area',
            values: [
              {
                reference: 'K02000001',
                description: 'United Kingdom',
                children: [
                  { reference: 'K03000001', description: 'Great Britain' },
                  { reference: 'E92000001', description: 'England' }
                ]
              }
            ]
          }
        ]
      }
    }
  }
};
