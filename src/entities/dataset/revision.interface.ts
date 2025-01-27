import { User } from '../user/user';

import { Dataset } from './dataset';
import { FactTable } from './fact-table';

export interface RevisionInterface {
    id: string;
    revisionIndex: number;
    dataset: Dataset;
    previousRevision: RevisionInterface;
    onlineCubeFilename: string;
    factTables: FactTable[];
    createdAt: Date;
    createdBy: User;
    approvedAt: Date | null;
    approvedBy: User | null;
    publishAt: Date;
}
