/* eslint-disable @typescript-eslint/naming-convention */
import { TranslationMap } from '../translate-openapi';

export const v2CyTranslations: TranslationMap = {
  info: {
    title: 'API cyhoeddus YstadegauCymru',
    description:
      "Bydd y dudalen hon yn eich helpu i ddefnyddio'r API cyhoeddus ar gyfer YstadegauCymru. Os oes angen unrhyw gymorth arall arnoch,\n      <a href=\"mailto:StatsWales@gov.wales\">cysylltwch ag YstadegauCymru</a>.\n\n<h2>Cychwyn cyflym</h2>\n<p><code>GET /{dataset_id}/data</code> yn dychwelyd pob rhes ar gyfer y diwygiad cyhoeddedig diweddaraf, wedi'i dudalennu gydag opsiynau arddangos rhagosodedig.</p>\n\n<h2>Hidlo data</h2>\n<ol>\n  <li><strong>Darganfod hidlyddion</strong> — <code>GET /{dataset_id}/filters</code> yn dychwelyd pob dimensiwn y gellir ei hidlo a'i werthoedd a ganiateir (enwau colofnau a chodau cyfeirio).</li>\n  <li><strong>Creu hidlydd</strong> — <code>POST /{dataset_id}/data</code> gyda chorff JSON sy'n cynnwys eich hidlyddion a'ch opsiynau arddangos dewisol. Yn dychwelyd <code>filter_id</code> (UUID) y gellir ei ailddefnyddio. Mae cyflwyno hidlyddion unfath yn dychwelyd yr un ID.</li>\n  <li><strong>Nôl data wedi'i hidlo</strong> — <code>GET /{dataset_id}/data/{filter_id}</code> yn dychwelyd rhesi tudalenedig sy'n cyd-fynd â'ch hidlydd.</li>\n</ol>\n\n<h2>Tablau colyn</h2>\n<ol>\n  <li><code>POST /{dataset_id}/pivot</code> gyda cholofnau echelin x/y a hidlyddion dewisol. Yn dychwelyd <code>filter_id</code>.</li>\n  <li><code>GET /{dataset_id}/pivot/{filter_id}</code> yn dychwelyd y golwg croes-dablu.</li>\n</ol>\n\n<h2>Opsiynau arddangos</h2>\n<p>Wrth greu hidlydd, gallwch gynnwys gwrthrych <code>options</code>:</p>\n<ul>\n  <li><code>use_raw_column_names</code> — pan fo'n <code>true</code> (rhagosodedig), mae penawdau colofnau'n defnyddio enwau tabl ffeithiau mewnol (e.e. <code>AreaCode</code>); pan fo'n <code>false</code>, maent yn defnyddio enwau dimensiynau darllenadwy (e.e. <code>Area</code>).</li>\n  <li><code>use_reference_values</code> — pan fo'n <code>true</code> (rhagosodedig), mae gwerthoedd celloedd yn godau cyfeirio (e.e. <code>K02000001</code>); pan fo'n <code>false</code>, maent yn ddisgrifiadau darllenadwy (e.e. <code>United Kingdom</code>).</li>\n  <li><code>data_value_type</code> — yn rheoli sut mae'r golofn gwerth data/mesur yn cael ei dychwelyd: <code>by_data_and_notes</code>, <code>by_data</code>, neu <code>by_notes</code>.</li>\n</ul>\n\n<h2>Iaith</h2>\n<p>Ychwanegwch <code>?lang=cy</code> at unrhyw gais i dderbyn labeli a disgrifiadau Cymraeg. Saesneg yw'r rhagosodiad (<code>en-gb</code>).</p>"
  },
  tags: {
    Datasets: 'Pori, chwilio, a chael metadata ar gyfer setiau data cyhoeddedig.',
    Topics: "Llywio'r hierarchaeth bynciau a ddefnyddir i gategoreiddio setiau data.",
    Data: 'Adalw data tabulaidd tudalenedig ar gyfer set ddata, gyda hidlo a threfnu dewisol.',
    Pivot: "Adalw golwg croes-dablu (tabl colyn) o ddata'r set ddata.",
    Query:
      "Archwilio ffurfweddiadau ymholiad wedi'u storio, gan gynnwys opsiynau hidlo, cyfrif rhesi, a mapio colofnau."
  },
  operations: {
    'GET /': {
      summary: "Cael rhestr o'r holl setiau data cyhoeddedig",
      description: "Yn dychwelyd rhestr dudalenedig o'r holl setiau data cyhoeddedig, wedi'u trefnu yn ôl y diweddaraf."
    },
    'GET /search': {
      summary: 'Chwilio setiau data cyhoeddedig',
      description:
        "Chwilio testun llawn ar draws teitlau a chrynodebau setiau data. Yn dychwelyd canlyniadau tudalenedig wedi'u graddio yn ôl perthnasedd."
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
      description:
        'Yn dychwelyd metadata llawn ar gyfer set ddata gyhoeddedig gan gynnwys manylion diwygio, amlder diweddaru, dynodiad, dolenni cysylltiedig, a phynciau.'
    },
    'GET /{dataset_id}/revision/{revision_id}': {
      summary: 'Cael diwygiad cyhoeddedig penodol yn ôl ID',
      description:
        'Yn dychwelyd metadata ar gyfer diwygiad cyhoeddedig penodol. Defnyddiwch y pwynt terfyn metadata set ddata i ddarganfod IDau diwygiad sydd ar gael.'
    },
    'GET /{dataset_id}/filters': {
      summary: 'Cael yr hidlyddion sydd ar gael ar gyfer set ddata',
      description:
        "Yn rhestru pob dimensiwn y gellir ei hidlo a'i werthoedd a ganiateir. Defnyddiwch yr enwau colofnau a'r codau cyfeirio a ddychwelwyd wrth greu hidlyddion gyda POST /data."
    },
    'POST /{dataset_id}/data': {
      summary: 'Cynhyrchu ID hidlydd ar gyfer ymholiad set ddata',
      description:
        "Yn storio hidlyddion rhesi ac opsiynau arddangos fel ymholiad y gellir ei ailddefnyddio. Yn dychwelyd ID hidlydd (UUID) i'w ddefnyddio gyda GET /data/{filter_id}. Mae cyflwyno hidlyddion unfath yn dychwelyd yr un ID."
    },
    'GET /{dataset_id}/data': {
      summary: 'Cael data tudalenedig ar gyfer set ddata',
      description:
        "Yn dychwelyd pob rhes ar gyfer y diwygiad cyhoeddedig diweddaraf, wedi'i dudalennu, gydag opsiynau arddangos rhagosodedig. Ar gyfer data wedi'i hidlo, crëwch hidlydd yn gyntaf gyda POST /data."
    },
    'POST /{dataset_id}/pivot': {
      summary: 'Cynhyrchu ID hidlydd ar gyfer ymholiad colyn',
      description:
        "Yn storio ffurfweddiad colyn (echelinau x/y) gyda hidlyddion ac opsiynau arddangos dewisol. Yn dychwelyd ID hidlydd i'w ddefnyddio gyda GET /pivot/{filter_id}."
    },
    'GET /{dataset_id}/data/{filter_id}': {
      summary: "Cael data tudalenedig ar gyfer set ddata gan ddefnyddio hidlydd wedi'i storio",
      description: "Yn dychwelyd data tudalenedig wedi'i hidlo a'i fformatio yn ôl yr ID hidlydd wedi'i storio."
    },
    'GET /{dataset_id}/pivot/{filter_id}': {
      summary: "Cael golwg colyn o set ddata gan ddefnyddio ID hidlydd wedi'i storio",
      description:
        "Yn dychwelyd golwg colyn croes-dablu gan ddefnyddio'r ffurfweddiad wedi'i storio yn yr ID hidlydd a roddwyd."
    },
    'GET /{dataset_id}/query/': {
      summary: 'Cael manylion yr ymholiad rhagosodedig ar gyfer set ddata',
      description:
        'Yn dychwelyd ffurfweddiad yr ymholiad rhagosodedig (heb ei hidlo), gan gynnwys cyfanswm cyfrif rhesi a mapio colofnau.'
    },
    'GET /{dataset_id}/query/{filter_id}': {
      summary: "Cael manylion ymholiad hidlydd wedi'i storio",
      description:
        "Yn dychwelyd ffurfweddiad llawn yr ymholiad wedi'i storio ar gyfer ID hidlydd — defnyddiol ar gyfer archwilio pa hidlyddion ac opsiynau sydd ar waith."
    }
  },
  responses: {
    'GET /': {
      '200': "Rhestr dudalenedig o'r holl setiau data cyhoeddedig"
    },
    'GET /search': {
      '200': "Rhestr dudalenedig o setiau data cyhoeddedig sy'n cyfateb"
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
    'GET /{dataset_id}/revision/{revision_id}': {
      '200': 'Metadata ar gyfer y diwygiad y gofynnwyd amdano'
    },
    'GET /{dataset_id}/filters': {
      '200': "Rhestr o ddimensiynau y gellir eu hidlo gyda'u gwerthoedd sydd ar gael"
    },
    'POST /{dataset_id}/data': {
      '200': 'Yr ID hidlydd a gynhyrchwyd'
    },
    'GET /{dataset_id}/data': {
      '200': "Golwg dudalenedig o ddata'r set ddata"
    },
    'POST /{dataset_id}/pivot': {
      '200': 'Yr ID hidlydd a gynhyrchwyd'
    },
    'GET /{dataset_id}/data/{filter_id}': {
      '200': "Golwg dudalenedig o ddata'r set ddata"
    },
    'GET /{dataset_id}/pivot/{filter_id}': {
      '200': "Golwg colyn dudalenedig o ddata'r set ddata"
    },
    'GET /{dataset_id}/query/': {
      '200': "Ffurfweddiad yr ymholiad wedi'i storio"
    },
    'GET /{dataset_id}/query/{filter_id}': {
      '200': "Ffurfweddiad yr ymholiad wedi'i storio"
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
    keywords: 'Llinyn ymholiad chwilio',
    revision_id: 'Dynodwr unigryw y diwygiad',
    filter_id: 'ID hidlydd a ddychwelwyd gan y pwynt terfyn POST /data neu POST /pivot'
  }
};
