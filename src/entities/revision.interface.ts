import { Dataset } from './dataset';
import { FileImport } from './file-import';
import { User } from './user';

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
    imports: Promise<FileImport[]>;
}
