/* eslint-disable @typescript-eslint/naming-convention */
import { TranslationMap } from '../translate-openapi';

export const v2CyTranslations: TranslationMap = {
  info: {
    title: 'API cyhoeddus YstadegauCymru',
    description:
      'Bydd y dudalen hon yn eich helpu i ddefnyddio\'r API cyhoeddus ar gyfer YstadegauCymru. Os oes angen unrhyw gymorth arall arnoch,\n      <a href="mailto:StatsWales@gov.wales">cysylltwch ag YstadegauCymru</a>.\n      <p>Nodyn: Gallwch ychwanegu <code>?lang=cy</code> at unrhyw gais i dderbyn labeli a disgrifiadau Cymraeg.</p>'
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
      description:
        "Yn dychwelyd rhestr dudalenedig o'r holl setiau data cyhoeddedig a'u IDau, wedi'u trefnu yn ôl y diweddaraf."
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
        "Yn dychwelyd metadata cyfredol ar gyfer set ddata gyhoeddedig, gan gynnwys crynodebau setiau data, pynciau a dolenni cysylltiedig. Gallwch gael IDau setiau data o'r pwyntiau terfyn 'Rhestrau o setiau data a phynciau'."
    },
    'GET /{dataset_id}/filters': {
      summary: 'Cael yr hidlyddion sydd ar gael ar gyfer set ddata',
      description:
        "<p>Yn dychwelyd rhestr o newidynnau mewn set ddata y gellir eu hidlo, a'r holl werthoedd y gellir eu hidlo ar gyfer pob newidyn.</p>  <p>Mae gan newidynnau:</p>  <ul>  <li>enw 'factTableColumn' a ddefnyddir wrth greu'r set ddata yn wreiddiol</li>  <li>'columnName' darllenadwy</li>  </ul>  <p>Mae gan werthoedd:</p>  <ul>  <li>cod 'reference'</li>  <li>'description' darllenadwy</li>  </ul>"
    },
    'POST /{dataset_id}/data': {
      summary: 'Cynhyrchu ID hidlydd ar gyfer ymholiad penodol',
      description:
        "<p>Yn cynhyrchu ID hidlydd ar gyfer cyfuniad dewisol o hidlyddion ac opsiynau arddangos. Mae'r ID bob amser yr un fath ar gyfer yr un cyfuniadau.</p>  <p>Mae angen i chi anfon corff JSON sy'n cynnwys adrannau ar gyfer 'filters' ac 'options'.</p>  <p>Dylai'r adran 'filters' gynnwys 'columnName' y newidyn a chodau 'reference' y gwerthoedd rydych am eu hidlo i mewn. Gallwch gael y rhain o'r pwynt terfyn 'Cael yr hidlyddion sydd ar gael ar gyfer set ddata'.</p>  <p>Dylai'r adran 'options' gynnwys y canlynol:</p>  <table>  <thead>  <tr>  <th>Opsiwn</th>  <th>Gwerth</th>  <th>Ystyr</th>  </tr>  </thead>  <tbody>  <tr>  <td rowspan='2'>use_raw_column_names</td>  <td>true [rhagosodedig]</td>  <td>Mae newidynnau'n defnyddio enwau 'factTableColumn', megis 'AreaCode'</td>  </tr>  <tr>  <td>false</td>  <td>Mae newidynnau'n defnyddio 'columnName' darllenadwy, megis 'Area'</td>  </tr>  <tr>  <td rowspan='2'>use_reference_values</td>  <td>true [rhagosodedig]</td>  <td>Mae gwerthoedd newidynnau'n defnyddio codau 'reference', megis 'K02000001'</td>  </tr>  <tr>  <td>false</td>  <td>Mae gwerthoedd newidynnau'n defnyddio 'description' darllenadwy, megis 'United Kingdom'</td>  </tr>  <tr>  <td rowspan='5'>data_value_type</td>  <td>raw [rhagosodedig]</td>  <td>Gwerthoedd data amrwd a dyddiadau</td>  </tr>  <tr>  <td>raw_extended</td>  <td>Gwerthoedd data amrwd a dyddiadau. Gyda cholofnau ychwanegol wedi'u hychwanegu at y tabl ar gyfer codau cyfeirio, hierarchaethau a chodau trefnu.</td>  </tr>  <tr>  <td>formatted</td>  <td>Gwerthoedd data wedi'u fformatio, gan gynnwys talgrynnu i leoedd degol a chomas i wahanu miloedd. Nid yw'n cynnwys dyddiadau wedi'u fformatio.</td>  </tr>  <tr>  <td>formatted_extended</td>  <td>Gwerthoedd data a dyddiadau wedi'u fformatio, gan gynnwys talgrynnu i leoedd degol a chomas i wahanu miloedd. Gyda cholofnau ychwanegol wedi'u hychwanegu at y tabl ar gyfer codau cyfeirio, hierarchaethau a chodau trefnu.</td>  </tr>  <tr>  <td>with_note_codes</td>  <td>Gwerthoedd data wedi'u hanodi â llaw-fer i roi manylder ychwanegol</td>  </tr>  </tbody>  </table>"
    },
    'GET /{dataset_id}/data': {
      summary: 'Cael data tudalenedig ar gyfer set ddata',
      description:
        "Yn dychwelyd rhesi ar gyfer y diwygiad cyhoeddedig diweddaraf fel arae JSON o wrthrychau. Mae gan bob gwrthrych enwau colofnau fel allweddi. Mae'r ymateb yn cynnwys pennawd Content-Disposition ar gyfer lawrlwytho. I gymhwyso hidlyddion, crëwch hidlydd yn gyntaf drwy POST /{dataset_id}/data, yna defnyddiwch GET /{dataset_id}/data/{filter_id}."
    },
    'POST /{dataset_id}/pivot': {
      summary: 'Cynhyrchu ID hidlydd ar gyfer ymholiad colyn penodol',
      description:
        "<p>Yn cynhyrchu ID hidlydd ar gyfer cyfuniad dewisol o ffurfweddiad colyn, hidlyddion ac opsiynau arddangos. Mae'r ID bob amser yr un fath ar gyfer yr un cyfuniadau.</p>  <p>Mae angen i chi anfon corff JSON sy'n cynnwys adrannau ar gyfer 'pivot', 'filters' ac 'options'.</p>  <p>Dylai'r adran 'pivot' gynnwys y newidynnau rydych am eu defnyddio ar gyfer:</p>  <ul>  <li>colofnau'r tabl colyn, neu echelin \"x\"</li>  <li>rhesi'r tabl colyn, neu echelin \"y\"</li>  </ul>  <p>Gallwch ddarganfod beth ddylai'r adrannau 'filters' ac 'options' gynnwys yn y pwynt terfyn 'Cynhyrchu ID hidlydd ar gyfer ymholiad penodol'.</p>"
    },
    'GET /{dataset_id}/data/{filter_id}': {
      summary: "Cael tabl data wedi'i hidlo ar gyfer set ddata",
      description:
        "Yn dychwelyd data cyfredol ar gyfer set ddata gyhoeddedig, wedi'i hidlo a'i arddangos yn ôl yr opsiynau a ddewiswyd ar gyfer ID hidlydd penodol."
    },
    'GET /{dataset_id}/pivot/{filter_id}': {
      summary: 'Cael tabl colyn ar gyfer set ddata',
      description:
        "Yn dychwelyd tabl colyn ar gyfer set ddata gyhoeddedig, wedi'i hidlo a'i arddangos yn ôl yr opsiynau a ddewiswyd ar gyfer ID hidlydd penodol."
    },
    'GET /{dataset_id}/query/{filter_id}': {
      summary: 'Cael manylion ymholiad hidlydd',
      description: "Yn dychwelyd yr opsiynau a'r ffurfweddiad a ddewiswyd ar gyfer ID hidlydd penodol."
    }
  },
  responses: {
    'GET /': {
      '200': "Rhestr dudalenedig o'r holl setiau data cyhoeddedig a'u IDau"
    },
    'GET /search': {
      '200': "Rhestr dudalenedig o setiau data cyhoeddedig sy'n cyfateb a'u IDau"
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
    'GET /{dataset_id}/filters': {
      '200': "Rhestr o ddimensiynau y gellir eu hidlo gyda'u gwerthoedd sydd ar gael"
    },
    'POST /{dataset_id}/data': {
      '200': 'Yr ID hidlydd a gynhyrchwyd'
    },
    'GET /{dataset_id}/data': {
      '200': 'Arae JSON o wrthrychau rhes data'
    },
    'POST /{dataset_id}/pivot': {
      '200': 'Yr ID hidlydd a gynhyrchwyd'
    },
    'GET /{dataset_id}/data/{filter_id}': {
      '200': "Arae JSON o wrthrychau rhes data wedi'u hidlo"
    },
    'GET /{dataset_id}/pivot/{filter_id}': {
      '200': "Golwg colyn dudalenedig o ddata'r set ddata"
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
    page_size: 'Nifer y gwerthoedd neu ganlyniadau fesul tudalen',
    sort_by:
      "Sut i drefnu'r data. Mae angen i chi gynnwys y `columnName` ac a yw'r golofn yn esgynnol neu'n ddisgynnol (`asc` neu `desc`). Mae'r cyfeiriad yn esgynnol yn ddiofyn. Gweler yr enghraifft am sut i fformatio hyn.",
    filter:
      "Priodweddau i hidlo'r data yn ôl. Dylai'r gwerth fod yn arae JSON o wrthrychau wedi'i anfon fel llinyn wedi'i amgodio URL.",
    keywords: 'Llinyn ymholiad chwilio',
    revision_id: 'Dynodwr unigryw y diwygiad',
    filter_id: 'ID hidlydd a ddychwelwyd gan y pwynt terfyn POST /data neu POST /pivot',
    search_mode:
      "Algorithm chwilio i'w ddefnyddio. **basic** (rhagosodedig): cyfatebiad is-linyn heb wahaniaethu llythrennau mawr/bach yn erbyn teitl a chrynodeb. **basic_split**: yn rhannu allweddeiriau'n eiriau unigol ac yn mynnu bod pob un yn ymddangos (rhesymeg AND). **fts**: chwilio testun llawn PostgreSQL gan ddefnyddio bôn-eiriau sy'n ymwybodol o iaith a graddio — yn dychwelyd meysydd `rank`, `match_title`, a `match_summary` gyda chyfatebiadau wedi'u hamlygu. **fts_simple**: fel fts ond yn defnyddio'r geiriadur 'syml' (dim bôn-eirio), defnyddiol ar gyfer chwiliadau Cymraeg. **fuzzy**: cyfatebiaeth tebygrwydd yn seiliedig ar drigram — yn goddef gwallau teipio a chyfatebiadau rhannol."
  }
};
