import { DeepPartial } from 'typeorm';

import { UserGroup } from '../../../entities/user/user-group';
import { Locale } from '../../../enums/locale';

// using hard coded uuids so that we can re-run the seeder for updates without creating new records
export const testGroup: DeepPartial<UserGroup> = {
  id: 'b080588c-86b0-46e1-87be-10776bc43743',
  organisationId: '4ef4facf-c488-4837-a65b-e66d4b525965', // Welsh Government
  metadata: [
    { name: 'E2E tests (cy)', email: 'e2ecy@example.com', language: Locale.WelshGb },
    { name: 'E2E tests', email: 'e2e@example.com', language: Locale.EnglishGb }
  ]
};
