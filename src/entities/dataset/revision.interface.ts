import { User } from '../user/user';

import { Dataset } from './dataset';
import { DataTable } from './data-table';

export interface RevisionInterface {
    id: string;
    revisionIndex: number;
    dataset: Dataset;
    previousRevision: RevisionInterface;
    onlineCubeFilename: string | null;
    dataTable: DataTable | null;
    createdAt: Date;
    createdBy: User;
    approvedAt: Date | null;
    approvedBy: User | null;
    publishAt: Date;
}
