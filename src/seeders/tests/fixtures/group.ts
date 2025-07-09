import { DeepPartial } from 'typeorm';

import { UserGroup } from '../../../entities/user/user-group';
import { Locale } from '../../../enums/locale';

// using hard coded uuids so that we can re-run the seeder for updates without creating new records
export const group1: DeepPartial<UserGroup> = {
  id: 'b080588c-86b0-46e1-87be-10776bc43743',
  organisationId: '4ef4facf-c488-4837-a65b-e66d4b525965', // Welsh Government
  metadata: [
    { name: 'E2E tests (cy)', email: 'e2ecy@example.com', language: Locale.WelshGb },
    { name: 'E2E tests', email: 'e2e@example.com', language: Locale.EnglishGb }
  ]
};

export const group2: DeepPartial<UserGroup> = {
  id: '7e5d1056-5568-4f64-80af-1f81323f04c8',
  organisationId: '4ef4facf-c488-4837-a65b-e66d4b525965', // Welsh Government
  metadata: [
    { name: 'E2E tests 2 (cy)', email: 'e2ecy@example.com', language: Locale.WelshGb },
    { name: 'E2E tests 2', email: 'e2e@example.com', language: Locale.EnglishGb }
  ]
};

export const group3: DeepPartial<UserGroup> = {
  id: 'ae6ee2e4-0726-403a-99b2-43d5f5a15f27',
  organisationId: '4ef4facf-c488-4837-a65b-e66d4b525965', // Welsh Government
  metadata: [
    { name: 'E2E tests 3 (cy)', email: 'e2ecy@example.com', language: Locale.WelshGb },
    { name: 'E2E tests 3', email: 'e2e@example.com', language: Locale.EnglishGb }
  ]
};

export const testGroups = [group1, group2, group3];
