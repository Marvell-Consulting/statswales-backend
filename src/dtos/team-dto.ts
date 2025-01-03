import { Team } from '../entities/user/team';
import { Locale } from '../enums/locale';

import { OrganisationDTO } from './organisation-dto';

export class TeamDTO {
    id: string;
    prefix?: string;
    name?: string;
    email?: string;
    organisation_id?: string;
    organisation?: OrganisationDTO;
    language?: string;

    static fromTeam(team: Team, lang: Locale): TeamDTO {
        const info = team.info?.find((i) => lang.includes(i.language));
        const dto = new TeamDTO();
        dto.id = team.id;
        dto.name = info?.name;
        dto.email = info?.email;
        dto.organisation_id = team.organisation?.id;
        dto.organisation = team.organisation ? OrganisationDTO.fromOrganisation(team.organisation, lang) : undefined;
        dto.language = lang;
        return dto;
    }
}
