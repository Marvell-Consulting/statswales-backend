import { Revision } from '../../src/entities/dataset/revision';
import { User } from '../../src/entities/user/user';
import { Locale } from '../../src/enums/locale';
import { DatasetRepository } from '../../src/repositories/dataset';
import { RevisionRepository } from '../../src/repositories/revision';
import { DatasetService } from '../../src/services/dataset';

jest.mock('../repositories/dataset');
jest.mock('../repositories/revision');

const user: Partial<User> = {
    id: '1234',
    email: 'test@example.com',
    name: 'Test User',
    provider: '',
    providerUserId: '',
    emailVerified: false,
    createdAt: new Date(),
    updatedAt: new Date()
};

const revision: Partial<Revision> = {
    id: '1234',
    revisionIndex: 1
};

describe('DatasetService', () => {
    // const datasetRepoMock = DatasetRepository as jest.Mock(DatasetRepository);

    // beforeEach(() => {
    //     DatasetRepository.mockClear();
    //     RevisionRepository.mockClear();
    // });

    it('can create a new dataset from scratch', async () => {
        const ds = new DatasetService(Locale.EnglishGb, user);
        const title = 'Test Dataset';
        await ds.createNew(title);

        expect(RevisionRepository.create).toHaveBeenCalledWith({ createdBy: user });
        expect(DatasetRepository.create).toHaveBeenCalledWith({
            createdBy: user,
            draftRevision: expect.any(Object),
            startRevision: expect.any(Object),
            endRevision: expect.any(Object)
        });
    });
});
