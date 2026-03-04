import { DeepPartial } from 'typeorm';

import { group1, group2, group3 } from './group';
import { User } from '../../../entities/user/user';
import { GlobalRole } from '../../../enums/global-role';
import { GroupRole } from '../../../enums/group-role';

type UserType = 'admin' | 'dev' | 'publisher' | 'approver' | 'solo';

const allGroups = [group1, group2, group3];

const groupRolesForType: Record<UserType, GroupRole[] | undefined> = {
  admin: undefined,
  dev: undefined,
  publisher: [GroupRole.Editor],
  approver: [GroupRole.Approver],
  solo: [GroupRole.Editor, GroupRole.Approver]
};

const globalRolesForType: Record<UserType, GlobalRole[] | undefined> = {
  admin: [GlobalRole.ServiceAdmin],
  dev: [GlobalRole.Developer],
  publisher: undefined,
  approver: undefined,
  solo: undefined
};

function makeUsers(type: UserType, ids: string[]): DeepPartial<User>[] {
  return ids.map((id, i) => {
    const index = i + 1;
    const user: DeepPartial<User> = {
      id,
      provider: 'local',
      providerUserId: `test_${type}_${index}`,
      name: `Test ${type.charAt(0).toUpperCase() + type.slice(1)} ${index}`,
      email: `test.${type}.${index}@example.com`
    };

    const globalRoles = globalRolesForType[type];
    if (globalRoles) {
      user.globalRoles = globalRoles;
    }

    const roles = groupRolesForType[type];
    if (roles) {
      user.groupRoles = allGroups.map((g) => ({ groupId: g.id, roles }));
    }

    return user;
  });
}

export const admins = makeUsers('admin', [
  '044d94c5-91ba-495e-a718-31c597a0a30b',
  'e0cbb352-f7ec-4d78-ac66-d8f84750065e',
  '11b7aa4d-e5e6-4de8-8403-c2cd42401884'
]);

export const devs = makeUsers('dev', [
  '2966170e-d88c-46fc-a8a8-f57826dec7e8',
  'ab10c4e6-ff19-40f5-b9c1-5a60bed93056',
  'be1cd621-00e9-42af-b205-ea2be46dc98e'
]);

export const publishers = makeUsers('publisher', [
  'f3dc1ae6-273e-4ac9-a498-ba2813c51c24',
  'd0d57b29-2cf7-4780-9440-9107c19f926d',
  '3556e694-ee64-443a-a1e5-9b92f1a40916',
  '1e8524a3-855f-4fe9-be5f-6f35534285be',
  '7ea50243-765e-479e-9a75-1582c7cbe6b8',
  'abfab535-b67e-4e9b-aaca-95d2b970949f',
  '6f1814c7-d37c-49e7-b9a7-f57ee7bee2b4',
  '69190ec3-3247-4e7e-a25c-a59534ca44c7',
  '2813f2af-0abd-4217-a607-b71304d0401b',
  '6c5dfa24-39b3-4a03-adef-8d3a468b5f94'
]);

export const approvers = makeUsers('approver', [
  'ce08727e-dd3f-48cc-921a-cae5c4dd4a18',
  'e4537822-6339-4236-bdb3-545518953bac',
  '5f65baa8-4cdc-4833-a0ce-e38a3fdfb024',
  'aa381510-0cbc-43f5-8938-516f5cb42b17',
  'ec3d62f4-f57a-452e-b372-8389a27c4893',
  'fe890ad0-646a-4cf8-8af7-2ef6da69bbba',
  'c5ed90c7-e9b0-4ede-a2ad-ff706c70141c',
  '2355d1b4-79ed-4ddb-ba84-3acbafc4c8df',
  '6e7fadb9-a6be-474a-9920-247c20e21905',
  'a8c19647-f110-46da-9c10-dfeb44e85f77'
]);

export const solos = makeUsers('solo', [
  '80d989fb-c26f-40dc-9a0b-6dc2083c0f0c',
  'e855e8c8-36f5-4f0d-bc24-85bc69f248fc',
  'c3b6b867-3246-47d7-ae0a-5dd35145be27',
  '5f3c4fdd-8359-4988-9719-e61bf1001ef3',
  '0523eb36-1752-4de3-af99-13a4557c07cd',
  'fd47af00-70db-431f-b5fe-2d1f6324240e',
  '5559f222-eb7a-458d-9b42-97c1644a8a99',
  '82ffc725-d6fd-4887-a1bd-60be2eb8a9d9',
  'b9d37d98-0cc8-451a-81a7-68d7de1738bc',
  'e4d6b401-3523-4563-9298-1035c2e3bfab'
]);

export const testUsers = [...admins, ...devs, ...publishers, ...approvers, ...solos];
