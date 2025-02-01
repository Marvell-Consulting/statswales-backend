import { Revision } from '../entities/dataset/revision';
import { DataTable } from '../entities/dataset/data-table';

import { DataTableDto } from './data-table-dto';

export class RevisionDTO {
    id: string;
    dataset_id?: string;
    revision_index: number;
    previous_revision_id?: string;
    online_cube_filename?: string;
    data_table?: DataTableDto;
    created_at: string;
    created_by: string;
    approved_at?: string;
    approved_by?: string;
    publish_at?: string;

    static fromRevision(revision: Revision): RevisionDTO {
        const revDto = new RevisionDTO();
        revDto.id = revision.id;
        revDto.revision_index = revision.revisionIndex;
        revDto.dataset_id = revision.dataset?.id;
        revDto.created_at = revision.createdAt.toISOString();
        revDto.previous_revision_id = revision.previousRevision?.id;
        revDto.online_cube_filename = revision.onlineCubeFilename || undefined;
        revDto.publish_at = revision.publishAt?.toISOString();
        revDto.approved_at = revision.approvedAt?.toISOString();
        revDto.approved_by = revision.approvedBy?.name;
        revDto.created_by = revision.createdBy?.name;
        if (revision.dataTable) {
            revDto.data_table = DataTableDto.fromDataTable(revision.dataTable);
        }
        return revDto;
    }
}
