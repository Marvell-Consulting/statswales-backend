// eslint-disable-next-line import/no-cycle
import { Dataset } from '../entities/dataset';
import { Import } from '../entities/import';
import { User } from '../entities/user';

export interface RevisionInterface {
    id: string;
    revisionIndex: number;
    dataset: Promise<Dataset>;
    creationDate: Date;
    previousRevision: Promise<RevisionInterface>;
    onlineCubeFilename: string;
    publishDate: Date;
    approvalDate: Date;
    approvedBy: Promise<User>;
    createdBy: Promise<User>;
    imports: Promise<Import[]>;
}
