import { DeepPartial } from 'typeorm';

import { Organisation } from '../../entities/user/organisation';
import { Team } from '../../entities/user/team';

// using hard coded uuids so that we can re-run the seeders idempotently

export const organisations: DeepPartial<Organisation>[] = [
    { id: '4ef4facf-c488-4837-a65b-e66d4b525965', nameEN: 'Welsh Government', nameCY: 'Awdurdod Refeniw Cymru' },
    { id: '51326112-33c1-4bdf-b51f-76f630ef4c48', nameEN: 'Welsh Revenue Authority', nameCY: 'Awdurdod Refeniw Cymru' }
];

export const teams: DeepPartial<Team>[] = [
    {
        id: 'd553156a-5555-4262-b7cb-4e62af9abd1a',
        prefix: 'AGRI',
        nameEN: 'Agriculture, fishing and forestry',
        nameCY: 'Amaethyddiaeth, pysgota a choedwigaeth',
        emailEN: 'stats.agric@gov.wales',
        emailCY: 'ystadegau.amaeth@llyw.cymru',
        organisation: organisations[0]
    },
    {
        id: 'c63bb6ed-60a6-4b21-bf3b-49ccbe094974',
        prefix: 'CSIW',
        nameEN: 'Care Inspectorate',
        nameCY: 'Arolygiaeth Gofal',
        emailEN: 'ciwinformation@gov.wales',
        emailCY: 'ystadegau.amaeth@llyw.cymru',
        organisation: organisations[0]
    },
    {
        id: 'ac17df6b-9024-415d-bff7-c6aae8059d84',
        prefix: 'ECON',
        nameEN: 'Economy',
        nameCY: `Yr economi a'r fachnad lafur`,
        emailEN: 'economic.stats@gov.wales',
        emailCY: 'ystadegau.economi@llyw.cymru',
        organisation: organisations[0]
    },
    {
        id: '6e831fdc-68b2-42f4-8f68-c45db0b07f61',
        prefix: 'ENVI',
        nameEN: 'Environment',
        nameCY: 'Yr amgylchedd',
        emailEN: 'stats.environment@gov.wales',
        emailCY: 'ystadegau.amgylchedd@llyw.cymru',
        organisation: organisations[0]
    },
    {
        id: '1207a8ab-1b62-4fa4-80b6-8c4c4a2b59b7',
        prefix: 'EQU',
        nameEN: 'Equality',
        nameCY: 'Cydraddoldeb',
        emailEN: 'stats.inclusion@gov.wales',
        emailCY: 'ystadegau.cynhwysiant@llyw.cymru',
        organisation: organisations[0]
    },
    {
        id: '903dc375-19e4-409b-b47e-55d4cbb88798',
        prefix: '',
        nameEN: 'Education',
        nameCY: '',
        emailEN: 'educationworkforcedata@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '15c928c2-2ae4-499e-a5d3-9597a8237f60',
        prefix: '',
        nameEN: 'Higher Education',
        nameCY: '',
        emailEN: 'highereducationandstudentfinance.stats@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'e53e2b3b-334f-4022-b7a2-483aa961c8e4',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'hss.performance@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '5fe64481-363e-454d-8f9b-52c6357922b8',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'labourmarket.stats@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'b21db89d-5abd-4393-abb5-0d37b79796fd',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'post16ed.stats@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'a28ce772-b12c-4eb7-b00c-56c055b19c7e',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'school.stats@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '3b78d57f-2a7a-4667-bc53-5d1145878a15',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.finance@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '438bdbba-eba8-45b4-84f5-bb7443ac64aa',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.healthinfo@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'd91cace9-662d-4dfe-99fe-87b1247dafe2',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.housing@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '08150366-3d5c-49fc-882a-0ebc2ad8181f',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.housingconditions@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '5cd706d1-d8e4-454c-a958-1d78a7d77317',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.info.desk@gov.wales',
        organisation: organisations[0]
    },
    {
        id: '59cd33df-2561-440c-80e8-f4f825b69893',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.popcensus@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'f07799c6-2b3d-44c0-a890-22d9ac999f48',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.pss@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'cd84d603-f8ea-4455-a827-1cffd13e7c3c',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.trade@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: 'b14e147b-7ecd-4a20-ab8b-57d5995b22cc',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'stats.transport@gov.wales',
        emailCY: '',
        organisation: organisations[0]
    },
    {
        id: '6006f94d-f64d-4db4-bac3-8a6c8e84adbe',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'surveys@gov.wales',
        organisation: organisations[0]
    },
    {
        id: 'c0424337-54fc-4299-826b-5367aa8f3814',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'tourismresearch@gov.wales',
        organisation: organisations[0]
    },
    {
        id: '4bcd05d7-5f0a-4621-82f0-49c04f68b0c3',
        prefix: '',
        nameEN: '',
        nameCY: '',
        emailEN: 'welshlanguagedata@gov.wales',
        organisation: organisations[0]
    },
    {
        id: '2b986d99-7753-47ec-89e8-4525ff97b7cf',
        prefix: 'WRAX',
        nameEN: 'Welsh Revenue Authority',
        nameCY: '',
        emailEN: 'data@wra.gov.wales',
        emailCY: 'ciwinformation@llyw.cymru',
        organisation: organisations[1]
    }
];
