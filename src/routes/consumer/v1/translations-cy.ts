/* eslint-disable @typescript-eslint/naming-convention */
import { TranslationMap, SchemaTranslation } from '../translate-openapi';

const schemaTranslations: Record<string, SchemaTranslation> = {
  Revision: {
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y diwygiad' },
      revision_index: { description: 'Rhif fersiwn, yn dechrau o 1' },
      dataset_id: { description: "Dynodwr unigryw ar gyfer y set ddata y mae'r diwygiad hwn yn perthyn iddi" },
      previous_revision_id: { description: 'Dynodwr unigryw ar gyfer y diwygiad blaenorol, os oes un' },
      created_at: { description: "Dyddiad creu'r diwygiad ar fformat ISO 8601" },
      updated_at: { description: 'Dyddiad diweddaru diwethaf y diwygiad ar fformat ISO 8601' },
      publish_at: { description: "Dyddiad cyhoeddi'r diwygiad ar fformat ISO 8601" },
      metadata: {
        description: 'Arae o barau allwedd-gwerth metadata ar gyfer y diwygiad',
        properties: {
          language: { description: 'Iaith y metadata, e.e. "en" ar gyfer Saesneg, "cy" ar gyfer Cymraeg' },
          title: { description: 'Teitl y diwygiad yn yr iaith a nodwyd' },
          summary: { description: "Crynodeb o'r diwygiad yn yr iaith a nodwyd" },
          collection: { description: "Enw'r casgliad ar gyfer y diwygiad yn yr iaith a nodwyd" },
          quality: { description: 'Gwybodaeth ansawdd ar gyfer y diwygiad yn yr iaith a nodwyd' },
          rounding_description: {
            description: "Disgrifiad o'r talgrynnu a gymhwyswyd i'r data yn y diwygiad hwn"
          }
        }
      },
      rounding_applied: { description: "Yn nodi a gymhwyswyd talgrynnu i'r data yn y diwygiad hwn" },
      update_frequency: {
        properties: {
          is_updated: { description: "Yn nodi a yw'r set ddata'n cael ei diweddaru'n rheolaidd" },
          frequency_value: { description: 'Gwerth rhifiadol amlder y diweddaru' },
          frequency_unit: { description: 'Uned amlder y diweddaru' }
        }
      },
      designation: { description: 'Dynodiad y diwygiad' },
      related_links: {
        properties: {
          id: { description: 'Dynodwr unigryw ar gyfer y ddolen gysylltiedig' },
          url: { description: 'URL y ddolen gysylltiedig' },
          label_en: { description: 'Label y ddolen gysylltiedig yn Saesneg' },
          label_cy: { description: 'Label y ddolen gysylltiedig yn Gymraeg' },
          created_at: { description: "Dyddiad creu'r ddolen gysylltiedig ar fformat ISO 8601" }
        }
      }
    }
  },
  Dataset: {
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y set ddata' },
      live: { description: 'Dyddiad cyhoeddi cyntaf y set ddata ar fformat ISO 8601' },
      start_date: { description: 'Dyddiad dechrau y set ddata ar fformat ISO 8601' },
      end_date: { description: 'Dyddiad diwedd y set ddata ar fformat ISO 8601' }
    }
  },
  DatasetListItem: {
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y set ddata' },
      title: {
        description: "Teitl y set ddata (yn yr iaith a ofynnwyd amdani drwy'r pennawd accept-language)"
      },
      first_published_at: { description: 'Dyddiad cyhoeddi cyntaf y set ddata ar fformat ISO 8601' },
      last_updated_at: { description: "Dyddiad y diweddariad diweddaraf i'r set ddata ar fformat ISO 8601" },
      archived_at: { description: "Dyddiad archifio'r set ddata ar fformat ISO 8601, os yn berthnasol" }
    }
  },
  DatasetsWithCount: {
    properties: {
      count: { description: 'Cyfanswm nifer y setiau data' }
    }
  },
  DatasetView: {
    properties: {
      current_page: { description: 'Rhif y dudalen gyfredol' },
      page_info: {
        properties: {
          total_records: { description: 'Cyfanswm nifer y cofnodion yn y set ddata' },
          start_record: { description: 'Rhif y cofnod cychwynnol ar gyfer y dudalen gyfredol' },
          end_record: { description: 'Rhif y cofnod olaf ar gyfer y dudalen gyfredol' }
        }
      },
      page_size: { description: 'Nifer y cofnodion fesul tudalen' },
      total_pages: { description: 'Cyfanswm nifer y tudalennau sydd ar gael' },
      headers: {
        properties: {
          index: { description: "Mynegai'r pennawd" },
          name: { description: "Enw'r pennawd" },
          source_type: { description: 'Math ffynhonnell y pennawd' }
        }
      },
      data: { description: 'Data tabulaidd ar gyfer y set ddata' }
    }
  },
  Topic: {
    properties: {
      id: { description: 'ID y pwnc' },
      path: { description: 'Llwybr y pwnc' },
      name: { description: 'Enw yn yr iaith gyfredol' },
      name_en: { description: "Enw'r pwnc yn Saesneg" },
      name_cy: { description: "Enw'r pwnc yn Gymraeg" }
    }
  },
  SubTopic: {
    properties: {
      id: { description: 'ID y pwnc' },
      path: { description: 'Llwybr y pwnc' },
      name: { description: 'Enw yn yr iaith gyfredol' },
      name_en: { description: "Enw'r pwnc yn Saesneg" },
      name_cy: { description: "Enw'r pwnc yn Gymraeg" }
    }
  },
  RootTopics: {},
  PublishedTopics: {}
};

export const v1CyTranslations: TranslationMap = {
  info: {
    title: 'API cyhoeddus YstadegauCymru',
    description:
      'Bydd y dudalen hon yn eich helpu i ddefnyddio\'r API cyhoeddus ar gyfer YstadegauCymru. Os oes angen unrhyw gymorth arall arnoch,\n      <a href="mailto:StatsWales@gov.wales">cysylltwch ag YstadegauCymru</a>.'
  },
  operations: {
    'GET /': {
      summary: "Cael rhestr o'r holl setiau data cyhoeddedig",
      description: "Mae'r pwynt terfyn hwn yn dychwelyd rhestr o'r holl setiau data cyhoeddedig."
    },
    'GET /topic': {
      summary: 'Cael rhestr o bynciau lefel uchaf',
      description:
        "Mae setiau data wedi'u tagio i bynciau. Mae pynciau lefel uchaf, megis 'Iechyd a gofal cymdeithasol', a all fod ag is-bynciau, megis 'Gwasanaethau deintyddol'. Mae'r pwynt terfyn hwn yn dychwelyd rhestr o'r holl bynciau lefel uchaf sydd ag o leiaf un set ddata gyhoeddedig wedi'i thagio iddynt."
    },
    'GET /topic/{topic_id}': {
      summary: "Cael rhestr o'r hyn sydd o dan bwnc penodol",
      description:
        "Mae setiau data wedi'u tagio i bynciau. Mae pynciau lefel uchaf, megis 'Iechyd a gofal cymdeithasol', a all fod ag is-bynciau, megis 'Gwasanaethau deintyddol'. Ar gyfer topic_id penodol, mae'r pwynt terfyn hwn yn dychwelyd rhestr o'r hyn sydd o dan y pwnc hwnnw - naill ai is-bynciau neu setiau data cyhoeddedig wedi'u tagio'n uniongyrchol i'r pwnc hwnnw."
    },
    'GET /{dataset_id}': {
      summary: 'Cael metadata set ddata gyhoeddedig',
      description: "Mae'r pwynt terfyn hwn yn dychwelyd yr holl fetadata ar gyfer set ddata gyhoeddedig."
    },
    'GET /{dataset_id}/view': {
      summary: 'Cael golwg tudalenedig o set ddata gyhoeddedig',
      description:
        "Mae'r pwynt terfyn hwn yn dychwelyd golwg tudalenedig o set ddata gyhoeddedig, gyda threfnu a hidlo dewisol."
    },
    'GET /{dataset_id}/view/filters': {
      summary: "Cael rhestr o'r hidlyddion sydd ar gael ar gyfer golwg tudalenedig o set ddata gyhoeddedig",
      description:
        "Mae'r pwynt terfyn hwn yn dychwelyd rhestr o'r hidlyddion sydd ar gael ar gyfer golwg tudalenedig o set ddata gyhoeddedig. Mae'r rhain yn seiliedig ar y newidynnau a ddefnyddir yn y set ddata, er enghraifft awdurdodau lleol neu flynyddoedd ariannol."
    },
    'GET /{dataset_id}/download/{format}': {
      summary: 'Lawrlwytho set ddata gyhoeddedig fel ffeil',
      description: "Mae'r pwynt terfyn hwn yn dychwelyd ffeil set ddata gyhoeddedig mewn fformat penodol."
    }
  },
  responses: {
    'GET /': {
      '200': "Rhestr dudalenedig o'r holl setiau data cyhoeddedig"
    },
    'GET /topic': {
      '200': "Rhestr o'r holl bynciau lefel uchaf sydd ag o leiaf un set ddata gyhoeddedig wedi'i thagio iddynt."
    },
    'GET /topic/{topic_id}': {
      '200':
        "Rhestr o'r hyn sydd o dan bwnc penodol - naill ai is-bynciau neu setiau data cyhoeddedig wedi'u tagio'n uniongyrchol i'r pwnc hwnnw."
    },
    'GET /{dataset_id}': {
      '200': "Gwrthrych JSON sy'n cynnwys yr holl fetadata ar gyfer set ddata gyhoeddedig"
    },
    'GET /{dataset_id}/view': {
      '200': 'Golwg tudalenedig o set ddata gyhoeddedig, gyda threfnu a hidlo dewisol'
    },
    'GET /{dataset_id}/view/filters': {
      '200': "Rhestr o'r hidlyddion sydd ar gael ar gyfer golwg tudalenedig o set ddata gyhoeddedig"
    },
    'GET /{dataset_id}/download/{format}': {
      '200': 'Ffeil set ddata gyhoeddedig mewn fformat penodol'
    }
  },
  parameters: {
    language:
      'Iaith i\'w defnyddio ar gyfer yr ymateb, "cy" neu "cy-gb" ar gyfer Cymraeg a "en" neu "en-gb" ar gyfer Saesneg',
    dataset_id: 'Dynodwr unigryw y set ddata a ddymunir',
    topic_id: 'Dynodwr unigryw y pwnc a ddymunir',
    format: 'Fformat ffeil ar gyfer y lawrlwythiad',
    page_number: 'Rhif tudalen ar gyfer tudalennu',
    page_size: 'Nifer y setiau data fesul tudalen',
    sort_by:
      "Colofnau i drefnu'r data yn ôl. Dylai'r gwerth fod yn arae JSON o wrthrychau wedi'i anfon fel llinyn wedi'i amgodio URL.",
    filter:
      "Priodweddau i hidlo'r data yn ôl. Dylai'r gwerth fod yn arae JSON o wrthrychau wedi'i anfon fel llinyn wedi'i amgodio URL.",
    view: 'Dewis a ddylid cynnwys colofnau ychwanegol megis codau cyfeirio a hierarchaethau yn y lawrlwythiad.'
  },
  schemas: schemaTranslations
};
