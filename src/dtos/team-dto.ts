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

    static fromTeam(team: Team, lang: Locale): TeamDTO {
        const dto = new TeamDTO();
        dto.id = team.id;
        dto.name = lang.includes('en') ? team.nameEN : team.nameCY;
        dto.email = lang.includes('en') ? team.emailEN : team.emailCY;
        dto.organisation_id = team.organisation?.id;
        dto.organisation = team.organisation ? OrganisationDTO.fromOrganisation(team.organisation, lang) : undefined;
        return dto;
    }
}
