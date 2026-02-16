import { DeepPartial } from 'typeorm';

import { group1, group2, group3 } from './group';
import { User } from '../../../entities/user/user';
import { GlobalRole } from '../../../enums/global-role';
import { GroupRole } from '../../../enums/group-role';

export const admin1: DeepPartial<User> = {
  id: '044d94c5-91ba-495e-a718-31c597a0a30b',
  provider: 'local',
  providerUserId: 'test_admin_1',
  name: 'Tom Admin',
  email: 'tom.admin@example.com',
  globalRoles: [GlobalRole.ServiceAdmin]
};

export const dev1: DeepPartial<User> = {
  id: '2966170e-d88c-46fc-a8a8-f57826dec7e8',
  provider: 'local',
  providerUserId: 'test_dev_1',
  name: 'Bob Developer',
  email: 'bob.developer@example.com',
  globalRoles: [GlobalRole.Developer]
};

// ── Publishers (Editor role) ────────────────────────────────────────────

export const publisher1: DeepPartial<User> = {
  id: 'f3dc1ae6-273e-4ac9-a498-ba2813c51c24',
  provider: 'local',
  providerUserId: 'test_publisher_1',
  name: 'Joe Publisher',
  email: 'joe.publisher@example.com',
  groupRoles: [
    { id: '56e78eb3-f831-48ff-ab78-8183acab9060', groupId: group1.id, roles: [GroupRole.Editor] },
    { id: '5c051ad8-5689-4427-ab7d-49c782d1fb8a', groupId: group2.id, roles: [GroupRole.Editor] },
    { id: '7f2d9f32-b282-4b28-85fd-31209059d8ab', groupId: group3.id, roles: [GroupRole.Editor] }
  ]
};

export const publisher2: DeepPartial<User> = {
  id: 'd0d57b29-2cf7-4780-9440-9107c19f926d',
  provider: 'local',
  providerUserId: 'test_publisher_2',
  name: 'Publisher Two',
  email: 'publisher.two@example.com',
  groupRoles: [
    { id: '923839f0-b0b6-434d-8833-7a9c77e4fd31', groupId: group1.id, roles: [GroupRole.Editor] },
    { id: '8447a8da-5261-4acd-ab8d-999da30a826d', groupId: group2.id, roles: [GroupRole.Editor] },
    { id: 'ef8d3900-6988-4c66-89f0-fa221d43d2cd', groupId: group3.id, roles: [GroupRole.Editor] }
  ]
};

export const publisher3: DeepPartial<User> = {
  id: '3556e694-ee64-443a-a1e5-9b92f1a40916',
  provider: 'local',
  providerUserId: 'test_publisher_3',
  name: 'Publisher Three',
  email: 'publisher.three@example.com',
  groupRoles: [
    { id: '8d840453-f7a7-4e9c-8121-5a8869af9161', groupId: group1.id, roles: [GroupRole.Editor] },
    { id: '5f18d686-f052-4f1f-93ab-582773ce51f8', groupId: group2.id, roles: [GroupRole.Editor] },
    { id: 'b5492d65-5e5d-49a6-a76e-d6801c3d9ea5', groupId: group3.id, roles: [GroupRole.Editor] }
  ]
};

export const publisher4: DeepPartial<User> = {
  id: '1e8524a3-855f-4fe9-be5f-6f35534285be',
  provider: 'local',
  providerUserId: 'test_publisher_4',
  name: 'Publisher Four',
  email: 'publisher.four@example.com',
  groupRoles: [
    { id: 'a843bd4d-df51-4b9d-991f-7c805d03a3b0', groupId: group1.id, roles: [GroupRole.Editor] },
    { id: '5133b18c-7dce-469b-90d5-31698963c254', groupId: group2.id, roles: [GroupRole.Editor] },
    { id: '73e71d85-85c8-447b-ae82-a996eeee8306', groupId: group3.id, roles: [GroupRole.Editor] }
  ]
};

// ── Approvers ───────────────────────────────────────────────────────────

export const approver1: DeepPartial<User> = {
  id: 'ce08727e-dd3f-48cc-921a-cae5c4dd4a18',
  provider: 'local',
  providerUserId: 'test_approver_1',
  name: 'Frank Approver',
  email: 'frank.approver@example.com',
  groupRoles: [
    { id: '347b302e-ea1a-40b2-ad02-6bf5f4b85bc6', groupId: group1.id, roles: [GroupRole.Approver] },
    { id: '3f977628-cbbc-4bb9-bb42-a928f3f59773', groupId: group2.id, roles: [GroupRole.Approver] },
    { id: '8f6b4f72-9f2c-4e47-9e9a-327a9a07663d', groupId: group3.id, roles: [GroupRole.Approver] }
  ]
};

export const approver2: DeepPartial<User> = {
  id: 'e4537822-6339-4236-bdb3-545518953bac',
  provider: 'local',
  providerUserId: 'test_approver_2',
  name: 'Approver Two',
  email: 'approver.two@example.com',
  groupRoles: [
    { id: '1c7e643b-ddb2-48a3-8380-30f547adf589', groupId: group1.id, roles: [GroupRole.Approver] },
    { id: '506effde-0b03-477c-950e-ce1efc017c31', groupId: group2.id, roles: [GroupRole.Approver] },
    { id: 'a5c2f9c8-a4b6-4442-8d4d-7c102571e234', groupId: group3.id, roles: [GroupRole.Approver] }
  ]
};

export const approver3: DeepPartial<User> = {
  id: '5f65baa8-4cdc-4833-a0ce-e38a3fdfb024',
  provider: 'local',
  providerUserId: 'test_approver_3',
  name: 'Approver Three',
  email: 'approver.three@example.com',
  groupRoles: [
    { id: '0deb359e-757c-43be-8324-4cacfb58c2bf', groupId: group1.id, roles: [GroupRole.Approver] },
    { id: '57c4d107-ee82-4552-875a-95a9ffff232e', groupId: group2.id, roles: [GroupRole.Approver] },
    { id: 'c1090fb5-652e-498c-a349-d9767e801583', groupId: group3.id, roles: [GroupRole.Approver] }
  ]
};

export const approver4: DeepPartial<User> = {
  id: 'aa381510-0cbc-43f5-8938-516f5cb42b17',
  provider: 'local',
  providerUserId: 'test_approver_4',
  name: 'Approver Four',
  email: 'approver.four@example.com',
  groupRoles: [
    { id: '1268a753-6c38-4af4-b4cf-13b192a54592', groupId: group1.id, roles: [GroupRole.Approver] },
    { id: '514c5298-280c-4f05-9980-1e590d7b8003', groupId: group2.id, roles: [GroupRole.Approver] },
    { id: 'a2c8c8f7-d0a1-44ee-b114-b6312fe53d29', groupId: group3.id, roles: [GroupRole.Approver] }
  ]
};

// ── Solos (Editor + Approver) ───────────────────────────────────────────

export const solo1: DeepPartial<User> = {
  id: '80d989fb-c26f-40dc-9a0b-6dc2083c0f0c',
  provider: 'local',
  providerUserId: 'test_solo_1',
  name: 'Solo Publisher Approver',
  email: 'solo.user@example.com',
  groupRoles: [
    { id: 'f9e6a489-1b34-4d86-9ee9-939df1b01078', groupId: group1.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '2bbef603-20c7-473a-9a87-aaa1d8c6d261', groupId: group2.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '0e8f1cee-c0e7-48e9-80d7-77faecf90a21', groupId: group3.id, roles: [GroupRole.Editor, GroupRole.Approver] }
  ]
};

export const solo2: DeepPartial<User> = {
  id: 'e855e8c8-36f5-4f0d-bc24-85bc69f248fc',
  provider: 'local',
  providerUserId: 'test_solo_2',
  name: 'Solo Two',
  email: 'solo.two@example.com',
  groupRoles: [
    { id: '789fd8c4-464f-4763-adde-d4e9fb9a2bbb', groupId: group1.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '8555d318-5d28-439c-97a8-ff5dab238b40', groupId: group2.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: 'b4e92078-f766-47fd-9b68-fc51ba80543a', groupId: group3.id, roles: [GroupRole.Editor, GroupRole.Approver] }
  ]
};

export const solo3: DeepPartial<User> = {
  id: 'c3b6b867-3246-47d7-ae0a-5dd35145be27',
  provider: 'local',
  providerUserId: 'test_solo_3',
  name: 'Solo Three',
  email: 'solo.three@example.com',
  groupRoles: [
    { id: 'b1aa47bb-20e6-4393-9948-09ca92567f94', groupId: group1.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '6d9233fe-0cf6-4f29-8db2-e9a7d993a42e', groupId: group2.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '5db037b9-f2ef-4324-b335-7597413d062a', groupId: group3.id, roles: [GroupRole.Editor, GroupRole.Approver] }
  ]
};

export const solo4: DeepPartial<User> = {
  id: '5f3c4fdd-8359-4988-9719-e61bf1001ef3',
  provider: 'local',
  providerUserId: 'test_solo_4',
  name: 'Solo Four',
  email: 'solo.four@example.com',
  groupRoles: [
    { id: '98206929-b81b-4c73-bc2b-aed5cd5b0816', groupId: group1.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '62603f7d-d01d-4add-9b2d-5df5620374b9', groupId: group2.id, roles: [GroupRole.Editor, GroupRole.Approver] },
    { id: '067b1d4f-c9cc-47f2-85d6-07ce7ba7a017', groupId: group3.id, roles: [GroupRole.Editor, GroupRole.Approver] }
  ]
};

export const testUsers = [
  admin1,
  dev1,
  publisher1,
  publisher2,
  publisher3,
  publisher4,
  approver1,
  approver2,
  approver3,
  approver4,
  solo1,
  solo2,
  solo3,
  solo4
];
