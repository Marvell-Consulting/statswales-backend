/* eslint-disable @typescript-eslint/naming-convention */
import { TranslationMap, SchemaTranslation } from '../translate-openapi';

const schemaTranslations: Record<string, SchemaTranslation> = {
  Revision: {
    properties: {
      id: { description: 'Dynodydd unigryw ar gyfer y diwygiad' },
      revision_index: { description: 'Rhif y fersiwn, gan gychwyn o 1' },
      dataset_id: { description: "Dynodydd unigryw ar gyfer y set ddata y mae'r diwygiad hwn yn perthyn iddi" },
      previous_revision_id: { description: 'Dynodydd unigryw ar gyfer y diwygiad blaenorol, os o gwbl' },
      created_at: { description: 'Dyddiad creu y diwygiad mewn fformat ISO 8601' },
      updated_at: { description: 'Dyddiad diweddariad diwethaf y diwygiad mewn fformat ISO 8601' },
      publish_at: { description: 'Dyddiad cyhoeddi y diwygiad mewn fformat ISO 8601' },
      metadata: {
        description: 'Aráe parau gwerth-allweddol metadata ar gyfer y diwygiad',
        properties: {
          language: { description: 'Iaith y metadata, e.e., "en" ar gyfer Saesneg, "cy" ar gyfer Cymraeg' },
          title: { description: 'Teitl y diwygiad yn yr iaith benodedig' },
          summary: { description: "Crynodeb o'r diwygiad yn yr iaith benodedig" },
          collection: { description: 'Enw casglu ar gyfer y diwygiad yn yr iaith benodedig' },
          quality: { description: 'Gwybodaeth o ansawdd ar gyfer y diwygiad yn yr iaith benodedig' },
          rounding_description: {
            description: "Disgrifiad o'r talgrynnu a weithredir i'r data yn y diwygiad hwn"
          }
        }
      },
      rounding_applied: { description: "Mae'n dynodi a wnaethpwyd gwaith talgrynnu i'r data yn y diwygiad hwn" },
      update_frequency: {
        properties: {
          is_updated: { description: "Mae'n dynodi a chaiff y set ddata ei diweddaru'n rheolaidd" },
          frequency_value: { description: 'Gwerth rhifol amlder y diwygio' },
          frequency_unit: { description: 'Uned amlder y diwygio' }
        }
      },
      designation: { description: 'Dynodiad y diwygiad' },
      related_links: {
        properties: {
          id: { description: 'Dynodydd unigryw ar gyfer y ddolen gysylltiedig' },
          url: { description: 'URL y ddolen gysylltiedig' },
          label_en: { description: 'Label y ddolen gysylltiedig yn Saesneg' },
          label_cy: { description: 'Label y ddolen gysylltiedig yn Gymraeg' },
          created_at: { description: 'Dyddiad creu y ddolen gysylltiedig mewn fformat ISO 8601' }
        }
      }
    }
  },
  Dataset: {
    properties: {
      id: { description: 'Dynodydd unigryw y set ddata' },
      live: { description: 'Dyddiad cyhoeddi cyntaf y set ddata mewn fformat ISO 8601' },
      start_date: { description: 'Dyddiad cychwyn y set ddata mewn fformat ISO 8601' },
      end_date: { description: 'Dyddiad gorffen y set ddata mewn fformat ISO 8601' }
    }
  },
  DatasetListItem: {
    properties: {
      id: { description: 'Dynodydd unigryw ar gyfer y set ddata' },
      title: {
        description: "Teitl y set ddata (yn yr iaith y gofynnwyd amdani trwy'r pennawd derbyn-iaith)"
      },
      first_published_at: { description: 'Dyddiad cyhoeddi cyntaf y set ddata mewn fformat ISO 8601' },
      last_updated_at: { description: "Dyddiad y diweddariad mwyaf diweddar i'r set ddata mewn fformat ISO 8601" },
      archived_at: { description: "Dyddiad pan archifwyd y set ddata mewn fformat ISO 8601, os yw hynny'n berthnasol" }
    }
  },
  DatasetsWithCount: {
    properties: {
      count: { description: 'Cyfanswm y setiau data' }
    }
  },
  DatasetView: {
    properties: {
      current_page: { description: 'Rhif y dudalen bresennol' },
      page_info: {
        properties: {
          total_records: { description: 'Cyfanswm y cofnodion yn y set ddata' },
          start_record: { description: 'Rhif cofnod cychwynnol ar gyfer y dudalen bresennol' },
          end_record: { description: 'Rhif cofnod olaf ar gyfer y dudalen bresennol' }
        }
      },
      page_size: { description: 'Nifer y cofnodion fesul tudalen' },
      total_pages: { description: 'Cyfanswm y tudalennau sydd ar gael' },
      headers: {
        properties: {
          index: { description: 'Mynegai y pennawd' },
          name: { description: "Enw'r pennawd" },
          source_type: { description: 'Math ffynhonnell y pennawd' }
        }
      },
      data: { description: 'Data tablaidd ar gyfer y set ddata' }
    }
  },
  Topic: {
    properties: {
      id: { description: 'ID y pwnc' },
      path: { description: 'Llwybr y pwnc' },
      name: { description: 'Enw yn yr iaith bresennol' },
      name_en: { description: "Enw'r pwnc yn Saesneg" },
      name_cy: { description: "Enw'r pwnc yn Gymraeg" }
    }
  },
  SubTopic: {
    properties: {
      id: { description: 'ID y pwnc' },
      path: { description: 'Llwybr y pwnc' },
      name: { description: 'Enw yn yr iaith bresennol' },
      name_en: { description: "Enw'r pwnc yn Saesneg" },
      name_cy: { description: "Enw'r pwnc yn Gymraeg" }
    }
  },
  RootTopics: {},
  PublishedTopics: {}
};

export const v1CyTranslations: TranslationMap = {
  info: {
    title: 'API cyhoeddus StatsCymru',
    description:
      'Bydd y dudalen hon yn eich helpu i ddefnyddio\'r API cyhoeddus ar gyfer StatsCymru. Os bydd angen unrhyw gymorth arall arnoch,\n      <a href="mailto:StatsWales@gov.wales">cysylltwch â StatsCymru</a>.'
  },
  operations: {
    'GET /': {
      summary: "Cael rhestr o'r holl setiau data cyhoeddedig",
      description: "Mae'r pwynt terfyn hwn yn rhoi rhestr o'r holl setiau data cyhoeddedig."
    },
    'GET /topic': {
      summary: "Cael rhestr o'r holl bynciau lefel uchaf",
      description:
        "Mae setiau data wedi cael eu tagio wrth bynciau. Ceir pynciau lefel uchaf, megis 'Iechyd a gofal cymdeithasol', sy'n gallu cynnwys is-bynciau, fel 'Gwasanaethau deintyddol'. Mae'r pwynt terfyn hwn yn rhoi rhestr o'r holl bynciau lefel uchaf y mae ganddynt o leiaf un set ddata gyhoeddedig wedi'i thagio wrthynt."
    },
    'GET /topic/{topic_id}': {
      summary: "Cael rhestr o'r hyn sy'n eistedd dan bwnc penodedig",
      description:
        "Mae setiau data wedi'u tagio wrth bynciau. Ceir pynciau lefel uchaf, megis 'Iechyd a gofal cymdeithasol', sy'n gallu cynnwys is-bynciau fel 'Gwasanaethau deintyddol'. Ar gyfer topic_id penodedig, mae'r pwynt terfyn hwn yn rhoi rhestr o'r hyn sy'n eistedd dan y pwnc hwnnw – naill ai is-bynciau neu setiau data cyhoeddedig wedi'u tagio yn uniongyrchol wrth y pwnc hwnnw."
    },
    'GET /{dataset_id}': {
      summary: 'Cael metadata set ddata gyhoeddedig',
      description: "Mae'r pwynt terfyn hwn yn rhoi'r holl fetadata ar gyfer set ddata gyhoeddedig."
    },
    'GET /{dataset_id}/view': {
      summary: 'Cael golwg ar ffurf tudalen o set ddata gyhoeddedig',
      description:
        "Mae'r pwynt terfyn hwn yn rhoi golwg ar ffurf tudalen o set ddata gyhoeddedig, gyda chyfleoedd dewisol i ddidoli a hidlo."
    },
    'GET /{dataset_id}/view/filters': {
      summary: "Cael rhestr o'r hidlwyr sydd ar gael ar gyfer golwg ar ffurf tudalen o set ddata gyhoeddedig",
      description:
        "Mae'r pwynt terfyn hwn yn rhoi rhestr o'r hidlwyr sydd ar gael ar gyfer golwg ar ffurf tudalen o set ddata gyhoeddedig. Mae'r rhain yn seiliedig ar y newidynnau a ddefnyddir yn y set ddata, er enghraifft awdurdodau lleol neu flynyddoedd ariannol."
    },
    'GET /{dataset_id}/download/{format}': {
      summary: 'Lawrlwytho set ddata gyhoeddedig fel ffeil',
      description: "Mae'r pwynt terfyn hwn yn rhoi ffeil set ddata gyhoeddedig mewn fformat penodedig."
    }
  },
  responses: {
    'GET /': {
      '200': "Rhestr ar ffurf tudalen o'r holl setiau data cyhoeddedig"
    },
    'GET /topic': {
      '200': "Rhestr o'r holl bynciau lefel uchaf sydd ag o leiaf un set ddata gyhoeddedig wedi'i thagio wrthynt."
    },
    'GET /topic/{topic_id}': {
      '200':
        "Rhestr o'r hyn sy'n eistedd dan bwnc penodedig – naill ai is-bynciau neu setiau data cyhoeddedig wedi'u tagio wrth y pwnc hwnnw yn uniongyrchol."
    },
    'GET /{dataset_id}': {
      '200': "Gwrthrych json sy'n cynnwys yr holl fetadata ar gyfer set ddata gyhoeddedig"
    },
    'GET /{dataset_id}/view': {
      '200': 'Golwg ar ffurf tudalen o set ddata gyhoeddedig, gyda chyfleoedd dewisol i ddidoli a hidlo'
    },
    'GET /{dataset_id}/view/filters': {
      '200': "Rhestr o'r hidlwyr sydd ar gael ar gyfer golwg ar ffurf tudalen o set ddata gyhoeddedig"
    },
    'GET /{dataset_id}/download/{format}': {
      '200': 'Ffeil set ddata gyhoeddedig mewn fformat penodedig'
    }
  },
  parameters: {
    language:
      'Iaith i\'w defnyddio ar gyfer yr ymateb, "cy" neu "cy-gb" ar gyfer Cymraeg ac "en" neu "en-gb" ar gyfer Saesneg',
    dataset_id: 'Dynodydd unigryw y set ddata a ddymunir',
    topic_id: 'Dynodydd unigryw y pwnc a ddymunir',
    format: 'Fformat ffeil y lawrlwythiad',
    page_number: 'Rhif y dudalen ar gyfer tudalennu',
    page_size: 'Nifer y setiau data fesul tudalen',
    sort_by:
      "Colofnau er mwyn didoli'r data. Dylai'r gwerth fod yn aráe JSON o wrthrychau a anfonir fel llinyn URL wedi'i amgodio.",
    filter:
      "Nodweddion er mwyn hidlo'r data. Dylai'r gwerth fod yn aráe JSON o wrthrychau a anfonir fel llinyn URL wedi'i amgodio.",
    view: 'Dewis a ddylid cynnwys colofnau ychwanegol megis codau cyfeirio a hierarchaethau yn y lawrlwythiad.'
  },
  schemas: schemaTranslations
};
