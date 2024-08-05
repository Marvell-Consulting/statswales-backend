import { User } from '../entity/user';

export class UserDto {
    id: string;
    oidcId: string;
    provider: string;
    name: string;
    email: string;
}

export function userToUserDTO(user: User): UserDto {
    return {
        id: user.id,
        oidcId: user.oidcId,
        provider: user.provider,
        name: user.name,
        email: user.email
    };
}
