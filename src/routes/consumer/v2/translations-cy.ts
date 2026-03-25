/* eslint-disable @typescript-eslint/naming-convention */
import { TranslationMap, SchemaTranslation } from '../translate-openapi';

const schemaTranslations: Record<string, SchemaTranslation> = {
  RevisionMetadata: {
    properties: {
      language: { description: 'Cod iaith y metadata, e.e. "en-GB" neu "cy-GB"' },
      title: { description: 'Teitl y diwygiad yn yr iaith a nodwyd' },
      summary: { description: "Crynodeb o'r diwygiad yn yr iaith a nodwyd" },
      collection: { description: "Enw'r casgliad ar gyfer y diwygiad yn yr iaith a nodwyd" },
      quality: { description: 'Gwybodaeth ansawdd ar gyfer y diwygiad yn yr iaith a nodwyd' },
      rounding_description: {
        description: "Disgrifiad o'r talgrynnu a gymhwyswyd i'r data yn y diwygiad hwn"
      },
      reason: { description: 'Rheswm dros y diweddariad, yn bresennol ar ddiwygiadau ar ôl y cyntaf' }
    }
  },
  UpdateFrequency: {
    properties: {
      update_type: {
        description:
          'Math o ddiweddariad: "update" ar gyfer data newydd wedi\'i ychwanegu, "replacement" ar gyfer amnewid llawn, "none" am ddim diweddariadau pellach'
      },
      date: {
        description: 'Dyddiad diweddaru disgwyliedig nesaf',
        properties: {
          day: { description: 'Diwrnod y mis' },
          month: { description: 'Rhif y mis' },
          year: { description: 'Blwyddyn pedwar digid' }
        }
      }
    }
  },
  RelatedLink: {
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y ddolen gysylltiedig' },
      url: { description: 'URL y ddolen gysylltiedig' },
      label_en: { description: 'Label y ddolen gysylltiedig yn Saesneg' },
      label_cy: { description: 'Label y ddolen gysylltiedig yn Gymraeg' },
      created_at: { description: "Dyddiad creu'r ddolen gysylltiedig ar fformat ISO 8601" }
    }
  },
  Provider: {
    properties: {
      language: { description: 'Cod iaith, e.e. "en-gb" neu "cy-gb"' },
      provider_name: { description: 'Enw darparwr y data' },
      source_name: { description: 'Enw ffynhonnell y data' }
    }
  },
  Revision: {
    description: "Diwygiad fel y'i dychwelir o fewn ymateb set ddata (pob iaith wedi'i chynnwys yn y metadata).",
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y diwygiad' },
      revision_index: { description: 'Rhif fersiwn, yn dechrau o 1' },
      previous_revision_id: { description: 'Dynodwr unigryw ar gyfer y diwygiad blaenorol, os oes un' },
      created_at: { description: "Dyddiad creu'r diwygiad ar fformat ISO 8601" },
      updated_at: { description: 'Dyddiad diweddaru diwethaf y diwygiad ar fformat ISO 8601' },
      approved_at: { description: "Dyddiad cymeradwyo'r diwygiad ar fformat ISO 8601" },
      publish_at: { description: "Dyddiad cyhoeddi'r diwygiad ar fformat ISO 8601" },
      unpublished_at: { description: "Dyddiad dadgyhoeddi'r diwygiad, os yn berthnasol" },
      coverage_start_date: {
        description:
          "Dechrau'r cyfnod amser a gwmpesir gan y data yn y diwygiad hwn, ar fformat ISO 8601. Yn bresennol ar gyfer setiau data â dimensiynau math dyddiad yn unig."
      },
      coverage_end_date: {
        description:
          'Diwedd y cyfnod amser a gwmpesir gan y data yn y diwygiad hwn, ar fformat ISO 8601. Yn bresennol ar gyfer setiau data â dimensiynau math dyddiad yn unig.'
      },
      metadata: { description: 'Metadata ar gyfer pob iaith (en-GB a cy-GB fel arfer)' },
      rounding_applied: {
        description: "Yn nodi a gymhwyswyd talgrynnu i'r data yn y diwygiad hwn"
      },
      designation: { description: 'Dynodiad ystadegol y diwygiad' },
      providers: { description: 'Darparwyr data a ffynonellau ar gyfer y diwygiad hwn' }
    }
  },
  SingleLanguageRevision: {
    description:
      "Diwygiad fel y'i dychwelir gan y pwynt terfyn /revision/:revision_id — mae'r metadata yn wrthrych sengl wedi'i hidlo i'r iaith a ofynnwyd amdani.",
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y diwygiad' },
      revision_index: { description: 'Rhif fersiwn, yn dechrau o 1' },
      previous_revision_id: { description: 'Dynodwr unigryw ar gyfer y diwygiad blaenorol, os oes un' },
      updated_at: { description: 'Dyddiad diweddaru diwethaf y diwygiad ar fformat ISO 8601' },
      publish_at: { description: "Dyddiad cyhoeddi'r diwygiad ar fformat ISO 8601" },
      coverage_start_date: { description: "Dechrau'r cyfnod amser a gwmpesir gan y data" },
      coverage_end_date: { description: 'Diwedd y cyfnod amser a gwmpesir gan y data' },
      metadata: { description: "Metadata wedi'i hidlo i'r iaith a ofynnwyd amdani" },
      rounding_applied: {
        description: "Yn nodi a gymhwyswyd talgrynnu i'r data yn y diwygiad hwn"
      },
      designation: { description: 'Dynodiad ystadegol y diwygiad' },
      providers: { description: "Darparwyr data a ffynonellau, wedi'u hidlo i'r iaith a ofynnwyd amdani" }
    }
  },
  Publisher: {
    properties: {
      group: {
        properties: {
          name: { description: "Enw'r grŵp cyhoeddi" },
          email: { description: 'E-bost cyswllt ar gyfer y grŵp cyhoeddi' }
        }
      },
      organisation: {
        properties: {
          name: { description: "Enw'r sefydliad cyhoeddi" }
        }
      }
    }
  },
  Dataset: {
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y set ddata' },
      first_published_at: { description: 'Dyddiad cyhoeddi cyntaf y set ddata ar fformat ISO 8601' },
      archived_at: {
        description: "Dyddiad archifio'r set ddata ar fformat ISO 8601, neu null os nad yw wedi'i harchifio"
      },
      replaced_by: {
        description:
          "Manylion y set ddata amnewid, os yw'r set ddata hon wedi'i harchifio gydag amnewid. Null os na nodwyd amnewid.",
        properties: {
          dataset_id: { description: "ID y set ddata sy'n disodli hon" },
          dataset_title: { description: 'Teitl y set ddata amnewid' },
          auto_redirect: {
            description: "Pan yn wir, dylai defnyddwyr gael eu hailgyfeirio i'r set ddata amnewid yn awtomatig"
          }
        }
      },
      start_date: {
        description:
          "Maes etifeddol — dechrau'r cyfnod amser a gwmpesir gan y set ddata. Heb ei osod ar gyfer setiau data mwy newydd; defnyddiwch coverage_start_date ar y diwygiad cyhoeddedig yn lle hynny."
      },
      end_date: {
        description:
          'Maes etifeddol — diwedd y cyfnod amser a gwmpesir gan y set ddata. Heb ei osod ar gyfer setiau data mwy newydd; defnyddiwch coverage_end_date ar y diwygiad cyhoeddedig yn lle hynny.'
      }
    }
  },
  DatasetListItem: {
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y set ddata' },
      title: { description: 'Teitl y set ddata yn yr iaith a ofynnwyd amdani' },
      first_published_at: { description: 'Dyddiad cyhoeddi cyntaf y set ddata ar fformat ISO 8601' },
      last_updated_at: {
        description: "Dyddiad y diweddariad diweddaraf i'r set ddata ar fformat ISO 8601"
      },
      archived_at: {
        description: "Dyddiad archifio'r set ddata ar fformat ISO 8601, neu null os nad yw wedi'i harchifio"
      }
    }
  },
  SearchResultItem: {
    description: "Canlyniad chwilio set ddata. Yn ymestyn DatasetListItem gyda meysydd sy'n benodol i chwilio.",
    properties: {
      id: { description: 'Dynodwr unigryw ar gyfer y set ddata' },
      title: { description: 'Teitl y set ddata' },
      summary: { description: "Crynodeb o'r set ddata" },
      rank: { description: 'Sgôr perthnasedd (yn bresennol ar gyfer moddau fts, fts_simple a fuzzy)' },
      match_title: {
        description:
          "Teitl gyda thermau chwilio wedi'u hamlygu mewn tagiau <mark> (yn bresennol ar gyfer moddau fts a fts_simple)"
      },
      match_summary: {
        description:
          "Crynodeb gyda thermau chwilio wedi'u hamlygu mewn tagiau <mark> (yn bresennol ar gyfer moddau fts a fts_simple)"
      }
    }
  },
  DatasetsWithCount: {
    properties: {
      count: { description: 'Cyfanswm nifer y setiau data' }
    }
  },
  SearchResultsWithCount: {
    properties: {
      count: { description: "Cyfanswm nifer y setiau data sy'n cyfateb" }
    }
  },
  DataRow: {
    description:
      'Rhes ddata sengl fel gwrthrych JSON. Mae allweddi yn enwau colofnau (enwau tabl ffeithiau yn ddiofyn) a gwerthoedd yn werthoedd data.'
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
  PublishedTopics: {
    properties: {
      children: {
        description:
          "Is-bynciau o dan y pwnc a ddewiswyd. Arae gwag os mai pwnc dail yw hwn (yn yr achos hwnnw bydd setiau data wedi'u llenwi)."
      },
      parents: { description: "Pynciau hynafiaid o'r gwraidd i'r pwnc a ddewiswyd" },
      datasets: {
        description:
          "Setiau data wedi'u tagio i'r pwnc hwn. Yn bresennol ar gyfer pynciau dail yn unig (pynciau heb blant)."
      }
    }
  },
  FilterValue: {
    properties: {
      reference: { description: "Cod cyfeirio i'w ddefnyddio mewn gwerthoedd hidlo" },
      description: { description: 'Label darllenadwy (yn dibynnu ar iaith)' },
      children: {
        description: 'Gwerthoedd plentyn ar gyfer dimensiynau hierarchaidd (e.e. Cymru → awdurdodau lleol)'
      }
    }
  },
  Filter: {
    description: "Dimensiwn hidladwy sengl a'i werthoedd a ganiateir.",
    properties: {
      factTableColumn: { description: 'Enw colofn fewnol y tabl ffeithiau, e.e. AreaCode' },
      columnName: {
        description: 'Enw dimensiwn darllenadwy — defnyddiwch hwn fel yr allwedd yn eich gwrthrych hidlo'
      }
    }
  },
  FilterId: {
    description:
      "Dynodwr ailddefnyddiadwy y gellir ei rannu ar gyfer set o hidlyddion ac opsiynau arddangos wedi'u storio. Mae'r un mewnbynnau hidlo bob amser yn cynhyrchu'r un ID.",
    properties: {
      filterId: {
        description:
          "Dynodwr 12 nod ar gyfer yr ymholiad wedi'i storio. Pasiwch hwn i GET /{dataset_id}/data/{filter_id} neu GET /{dataset_id}/pivot/{filter_id} i gael canlyniadau wedi'u hidlo."
      }
    }
  },
  DataOptions: {
    description:
      "Hidlyddion rhesi ac opsiynau arddangos i'w storio fel ymholiad ailddefnyddiadwy. Defnyddiwch enwau colofnau a chodau cyfeirio o GET /{dataset_id}/filters.",
    properties: {
      filters: {
        description:
          "Mae gan bob gwrthrych un allwedd (enw colofn o GET /filters) wedi'i mapio i arae o godau cyfeirio. Mae gwrthrychau lluosog yn cyfuno â rhesymeg AND; mae gwerthoedd lluosog o fewn un gwrthrych yn cyfuno â rhesymeg OR."
      },
      options: {
        description:
          'Opsiynau arddangos. Os heb eu darparu, y rhagosodiadau yw use_raw_column_names: true, use_reference_values: true, data_value_type: raw.',
        properties: {
          use_raw_column_names: {
            description:
              "Pan yn wir (rhagosodedig), mae penawdau colofnau'n defnyddio enwau mewnol y tabl ffeithiau (e.e. AreaCode). Pan yn ffug, mae penawdau'n defnyddio enwau dimensiwn darllenadwy (e.e. Area)."
          },
          use_reference_values: {
            description:
              'Pan yn wir (rhagosodedig), mae gwerthoedd celloedd yn godau cyfeirio (e.e. K02000001). Pan yn ffug, mae gwerthoedd yn ddisgrifiadau darllenadwy (e.e. United Kingdom).'
          },
          data_value_type: {
            description:
              "Yn dewis y golwg ciwb a ddefnyddir ar gyfer allbwn data. raw (rhagosodedig): gwerthoedd data amrwd a dyddiadau. raw_extended: gwerthoedd amrwd ynghyd â chodau cyfeirio, hierarchaethau, a threfnau trefnu. formatted: gwerthoedd data wedi'u fformatio, dim dyddiadau. formatted_extended: gwerthoedd a dyddiadau wedi'u fformatio ynghyd â chodau cyfeirio, hierarchaethau, a threfnau trefnu. with_note_codes: gwerthoedd data wedi'u hanodi â marcwyr nodiadau."
          }
        }
      }
    }
  },
  PivotOptions: {
    properties: {
      pivot: {
        properties: {
          x: { description: "Enw'r golofn ar gyfer echelin lorweddol y tabl colyn" },
          y: { description: "Enw'r golofn ar gyfer echelin fertigol y tabl colyn" },
          backend: { description: "Peiriant ôl-ben i'w ddefnyddio ar gyfer y colyn (rhagosodedig: duckdb)" },
          include_performance: { description: 'Cynnwys metadata perfformiad yn yr ymateb' }
        }
      }
    }
  },
  QueryStore: {
    properties: {
      id: { description: "Dynodwr 12 nod ar gyfer yr ymholiad wedi'i storio" },
      hash: { description: "Hash o baramedrau'r ymholiad ar gyfer dyblygu" },
      datasetId: { description: "Y set ddata y mae'r ymholiad hwn yn perthyn iddi" },
      revisionId: { description: "Y diwygiad y mae'r ymholiad hwn yn perthyn iddo" },
      query: { description: 'Map allwedd-gwerth o god iaith i linyn ymholiad SQL' },
      totalLines: { description: "Cyfanswm nifer y rhesi sy'n cyfateb i'r ymholiad" },
      columnMapping: {
        description: "Mapio enwau colofnau'r tabl ffeithiau i enwau arddangos dimensiynau"
      }
    }
  }
};

export const v2CyTranslations: TranslationMap = {
  info: {
    title: 'API cyhoeddus StatsCymru',
    description:
      'Bydd y dudalen hon yn eich helpu i ddefnyddio\'r API cyhoeddus ar gyfer StatsCymru. Os bydd angen unrhyw gymorth arall arnoch,\n      <a href="mailto:StatsWales@gov.wales">cysylltwch â StatsCymru</a>.\n      <p>Sylwer: Gallwch ychwanegu <code>?lang=cy</code> i unrhyw gais er mwyn cael labeli a disgrifiadau Cymraeg.</p>'
  },
  tags: {
    Datasets: 'Pori, chwilio ac adalw metadata ar gyfer setiau data cyhoeddedig.',
    Topics: "Gwe-lywio'r hierarchaeth o ran pynciau a ddefnyddir i gategoreiddio setiau data.",
    Data: "Adalw data tablaidd wedi'i dudalennu ar gyfer set ddata, gyda dewis hidlo a didoli.",
    Pivot: "Adalw golwg wedi'i groes-dablu (tabl colynnu) o ddata set ddata.",
    Query:
      "Archwilio ffurfweddiadau ymholi wedi'u storio, gan gynnwys dewisiadau hidlo, cyfrif rhesi, a mapio colofnau."
  },
  operations: {
    'GET /': {
      summary: "Cael rhestr o'r holl setiau data cyhoeddedig",
      description:
        "Dychwelyd rhestr wedi'i thudalennu o'r holl setiau data cyhoeddedig a'u manylion adnabod, wedi'u rhoi mewn trefn yn ôl y rhai a ddiweddarwyd yn fwyaf diweddar."
    },
    'GET /search': {
      summary: 'Chwilio setiau data cyhoeddedig',
      description:
        "Chwiliad testun llawn ar draws crynodebau a theitlau setiau data. Mae'n rhoi canlyniadau wedi'u tudalennu a'u rhestru yn ôl eu perthnasedd."
    },
    'GET /topic': {
      summary: 'Cael rhestr o bynciau lefel uchaf',
      description:
        "Caiff setiau data eu tagio wrth bynciau. Pynciau lefel uchaf yw'r rhain, fel 'Iechyd a gofal cymdeithasol', sy'n gallu cael is-bynciau, fel 'Gwasanaethau deintyddol'. Mae'r pwynt terfyn hwn yn rhoi rhestr o'r holl bynciau lefel uchaf sydd ag o leiaf un set ddata gyhoeddedig wedi'i thagio wrthynt."
    },
    'GET /topic/{topic_id}': {
      summary: "Cael rhestr o'r hyn sy'n eistedd dan bwnc penodedig",
      description:
        "Caiff setiau data eu tagio wrth bynciau. Ceir pynciau lefel uchaf, fel 'Iechyd a gofal cymdeithasol', sy'n gallu cael is-bynciau, fel 'Gwasanaethau deintyddol'. Ar gyfer topic_id penodol, mae'r pwynt terfyn hwn yn rhoi rhestr o'r hyn sy'n eistedd dan y pwnc hwnnw – naill ai is-bynciau neu setiau data cyhoeddedig wedi'u tagio wrth y pwnc hwnnw yn uniongyrchol."
    },
    'GET /{dataset_id}': {
      summary: 'Cael metadata set ddata gyhoeddedig',
      description:
        "Mae'n dychwelyd metadata cyfredol ar gyfer set ddata gyhoeddedig, gan gynnwys crynodebau o setiau data, pynciau a dolenni cysylltiedig. Gallwch gael manylion adnabod set ddata o bwyntiau terfyn 'Rhestrau setiau data a phynciau'."
    },
    'GET /{dataset_id}/filters': {
      summary: 'Cael hidlwyr sydd ar gael ar gyfer set ddata',
      description:
        "<p>Mae'n dychwelyd rhestr o newidynnau mewn set ddata, y mae modd eu hidlo, a phob gwerth hidladwy ar gyfer pob newidyn.</p>  <p>Mae gan newidynnau:</p>  <ul>  <li>enw 'factTableColumn' a ddefnyddir pan gaiff y set ddata ei chreu yn y lle cyntaf</li>  <li>'columnName' y gall rhywun ei ddarllen</li>  </ul>  <p>Mae gan werthoedd:</p>  <ul>  <li>cod 'reference'</li>  <li>'description' y gall rhywun ei ddarllen</li>  </ul>"
    },
    'POST /{dataset_id}/data': {
      summary: 'Creu ID hidlo ar gyfer ymholiad penodol',
      description:
        "<p>Yn creu ID hidlo ar gyfer cyfuniad a ddewisir o hidlwyr a dewisiadau arddangos. Mae'r ID hwn wastad yr un fath ar gyfer yr un cyfuniadau.</p>  <p>Mae angen i chi anfon corff JSON sy'n cynnwys adrannau ar gyfer 'filters' ac 'options'.</p>  <p>Dylai'r adran 'filters' gynnwys 'columnName' y newidyn a'r codau 'reference' ar gyfer y gwerthoedd yr ydych yn dymuno eu hidlo i mewn. Gallwch gael y rhain o'r pwynt terfyn 'Cael hidlwyr sydd ar gael ar gyfer set ddata'.</p>  <p>Dylai'r adran 'options' gynnwys y canlynol:</p>  <table>  <thead>  <tr>  <th>Dewis</th>  <th>Gwerth</th>  <th>Ystyr</th>  </tr>  </thead>  <tbody>  <tr>  <td rowspan='2'>use_raw_column_names</td>  <td>true [diofyn]</td>  <td>Mae newidynnau yn defnyddio enwau 'factTableColumn', megis 'AreaCode'</td>  </tr>  <tr>  <td>false</td>  <td>Mae newidynnau yn defnyddio 'columnName' y gall rhywun ei ddarllen, fel 'Area'</td>  </tr>  <tr>  <td rowspan='2'>use_reference_values</td>  <td>true [diofyn]</td>  <td>Mae gwerthoedd newidynnau yn defnyddio codau 'reference', megis 'K02000001'</td>  </tr>  <tr>  <td>false</td>  <td>Mae gwerthoedd newidynnau yn defnyddio 'description' y gall rhywun ei ddarllen, fel 'Y Deyrnas Unedig'</td>  </tr>  <tr>  <td rowspan='5'>data_value_type</td>  <td>raw [diofyn]</td>  <td>Dyddiadau a gwerthoedd data amrwd</td>  </tr>  <tr>  <td>raw_extended</td>  <td>Dyddiadau a gwerthoedd data amrwd. A cholofnau ychwanegol wedi'u hychwanegu i'r tabl ar gyfer codau cyfeirio, hierarchaethau a chodau didoli.</td>  </tr>  <tr>  <td>formatted</td>  <td>Gwerthoedd data wedi'u fformatio, gan gynnwys talgrynnu i leoedd degol a chomas i wahanu miloedd. Nid yw'n cynnwys dyddiadau wedi'u fformatio.</td>  </tr>  <tr>  <td>formatted_extended</td>  <td>Dyddiadau a gwerthoedd data wedi'u fformatio, gan gynnwys talgrynnu i leoedd degol a chomas i wahanu miloedd. A cholofnau ychwanegol wedi'u hychwanegu i'r tabl ar gyfer codau cyfeirio, hierarchaethau a chodau didoli.</td>  </tr>  <tr>  <td>with_note_codes</td>  <td>Gwerthoedd data wedi'u hanodi â llaw-fer er mwyn cynnig manylion ychwanegol</td>  </tr>  </tbody>  </table>"
    },
    'GET /{dataset_id}/data': {
      summary: "Cael data wedi'i dudalennu ar gyfer set ddata",
      description:
        "Mae'n rhoi rhesi ar gyfer y diwygiad cyhoeddedig diweddaraf fel arae JSON o wrthrychau. Mae gan bob gwrthrych enwau colofnau fel allweddi. Mae'r ymateb yn cynnwys pennawd Content-Disposition i'w lawrlwytho. I ddefnyddio hidlwyr, yn gyntaf, crëwch hidlydd trwy POST /{dataset_id}/data, yna defnyddiwch GET /{dataset_id}/data/{filter_id}."
    },
    'POST /{dataset_id}/pivot': {
      summary: 'Creu ID hidlo ar gyfer ymholiad colynnu penodol',
      description:
        "<p>Mae'n creu ID hidlo ar gyfer cyfuniad a ddewisir o ffurfweddiad colynnu, hidlwyr a dewisiadau arddangos. Mae'r ID hwn wastad yr un fath ar gyfer yr un cyfuniadau.</p>  <p>Mae angen i chi anfon corff JSON sy'n cynnwys adrannau ar gyfer 'pivot', 'filters' ac 'options'.</p>  <p>Dylai'r adran 'pivot' gynnwys y newidynnau yr ydych chi'n dymuno eu defnyddio ar gyfer:</p>  <ul>  <li>colofnau'r tabl colynnu, neu echelin \"x\"</li>  <li>rhesi'r tabl colynnu, neu echelin \"y\"</li>  </ul>  <p>Gallwch weld yr hyn y dylai'r adrannau 'filters' ac 'options' eu cynnwys ym mhwynt terfyn 'Creu ID hidlo ar gyfer ymholiad penodol'.</p>"
    },
    'GET /{dataset_id}/data/{filter_id}': {
      summary: "Cael tabl data wedi'i hidlo ar gyfer set ddata",
      description:
        "Mae'n dychwelyd data cyfredol ar gyfer set ddata gyhoeddedig, wedi'i hidlo a'i ddangos yn ôl y dewisiadau a ddewiswyd ar gyfer ID hidlo penodol."
    },
    'GET /{dataset_id}/pivot/{filter_id}': {
      summary: 'Cael tabl colynnu ar gyfer set ddata',
      description:
        "Yn dychwelyd tabl colynnu ar gyfer set ddata gyhoeddedig, wedi'i hidlo a'i arddangos yn unol â'r dewisiadau a ddewiswyd ar gyfer ID hidlo penodol."
    },
    'GET /{dataset_id}/query/{filter_id}': {
      summary: 'Cael manylion ymholiad hidlo',
      description: "Yn rhoi'r dewisiadau a'r ffurfweddiad a ddewiswyd ar gyfer ID hidlo penodol."
    }
  },
  responses: {
    'GET /': {
      '200': "Rhestr wedi'i thudalennu o'r holl setiau data cyhoeddedig a'u manylion adnabod."
    },
    'GET /search': {
      '200': "Rhestr wedi'i thudalennu o setiau data cyhoeddedig sy'n cyfateb, a'u manylion adnabod."
    },
    'GET /topic': {
      '200': "Rhestr o'r holl bynciau lefel uchaf sydd ag o leiaf un set ddata gyhoeddedig wedi'i thagio wrthynt."
    },
    'GET /topic/{topic_id}': {
      '200':
        "Rhestr o'r hyn sy'n eistedd dan bwnc penodedig – naill ai is-bynciau neu setiau data cyhoeddedig wedi'u tagio wrth y pwnc hwnnw yn uniongyrchol."
    },
    'GET /{dataset_id}': {
      '200': "Gwrthrych JSON sy'n cynnwys yr holl fetadata ar gyfer set ddata gyhoeddedig"
    },
    'GET /{dataset_id}/filters': {
      '200': "Rhestr o ddimensiynau hidladwy gyda'u gwerthoedd sydd ar gael"
    },
    'POST /{dataset_id}/data': {
      '200': 'Yr ID hidlo a luniwyd'
    },
    'GET /{dataset_id}/data': {
      '200': 'Arae JSON o wrthrychau rhesi data'
    },
    'POST /{dataset_id}/pivot': {
      '200': 'Yr ID hidlo a luniwyd'
    },
    'GET /{dataset_id}/data/{filter_id}': {
      '200': "Arae JSON o wrthrychau rhesi data wedi'u hidlo"
    },
    'GET /{dataset_id}/pivot/{filter_id}': {
      '200': "Golwg colynnu wedi'i dudalennu o ddata y set ddata"
    },
    'GET /{dataset_id}/query/{filter_id}': {
      '200': 'Ffurfweddiad yr ymholiad a storiwyd'
    }
  },
  parameters: {
    language:
      'Iaith i\'w defnyddio ar gyfer yr ymateb, "cy" neu "cy-gb" ar gyfer Cymraeg ac "en" neu "en-gb" ar gyfer Saesneg',
    dataset_id: 'Dynodwr unigryw y set ddata ddymunol',
    topic_id: 'Dynodwr unigryw y pwnc dymunol',
    format: 'Fformat ffeil ar gyfer y lawrlwythiad',
    page_number: 'Rhif y dudalen er mwyn tudalennu',
    page_size: 'Nifer y gwerthoedd neu ganlyniadau fesul tudalen',
    sort_by:
      "Sut i ddidoli'r data. Mae angen i chi gynnwys y `columnName` ac a yw'r golofn yn esgyn neu'n disgyn (`asc` neu `desc`). Y cyfeiriad diofyn yw esgyn. Gweler yr enghraifft am sut i fformatio hyn.",
    filter:
      "Nodweddion er mwyn hidlo'r data. Dylai'r gwerth fod yn arae JSON o wrthrychau a anfonwyd fel llinyn URL wedi'i amgodio.",
    keywords: 'Chwilio llinyn ymholiad',
    revision_id: 'Dynodwr unigryw y diwygiad',
    filter_id: "ID hidlo wedi'i ddychwelyd gan y pwynt terfyn POST /{dataset_id}/data neu POST /{dataset_id}/pivot",
    search_mode:
      "Algorithm chwilio i'w ddefnyddio. **basic** (diofyn): paru is-linyn heb fod yn sensitif i faint llythrennau yn erbyn teitl a chrynodeb. **basic_split**: yn rhannu geiriau allweddol yn eiriau unigol ac yn mynnu eu bod oll yn ymddangos (rhesymeg AND). **fts**: chwiliad testun llawn PostgreSQL gan ddefnyddio bôn-eiriau ymwybodol o iaith a graddio — yn dychwelyd meysydd `rank`, `match_title`, a `match_summary` gyda pharu wedi'u hamlygu. **fts_simple**: fel fts ond yn defnyddio'r geiriadur 'simple' (dim bôn-eirio), sy'n ddefnyddiol ar gyfer chwiliadau Cymraeg. **fuzzy**: paru tebygrwydd yn seiliedig ar drigram — yn goddef camgymeriadau teipio a pharu rhannol."
  },
  schemas: schemaTranslations
};
