import { Dataset } from './dataset';
import { FileImport } from './file-import';
import { User } from './user';

export interface RevisionInterface {
    id: string;
    revisionIndex: number;
    dataset: Promise<Dataset>;
    previousRevision: Promise<RevisionInterface>;
    onlineCubeFilename: string;
    imports: Promise<FileImport[]>;
    createdAt: Date;
    createdBy: Promise<User>;
    approvedAt: Date;
    approvedBy: Promise<User>;
    publishAt: Date;
}
