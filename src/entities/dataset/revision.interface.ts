import { User } from '../user/user';

import { Dataset } from './dataset';
import { FileImport } from './file-import';

export interface RevisionInterface {
    id: string;
    revisionIndex: number;
    dataset: Dataset;
    previousRevision: RevisionInterface;
    onlineCubeFilename: string;
    imports: FileImport[];
    createdAt: Date;
    createdBy: User;
    approvedAt: Date;
    approvedBy: User;
    publishAt: Date;
}
