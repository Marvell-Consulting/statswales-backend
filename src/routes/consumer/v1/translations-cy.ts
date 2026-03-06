/* eslint-disable @typescript-eslint/naming-convention */
import { TranslationMap } from '../translate-openapi';

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
  }
};
