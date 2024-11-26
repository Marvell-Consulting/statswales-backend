import { Revision } from '../entities/dataset/revision';
import { FactTable } from '../entities/dataset/fact-table';
import { FactTableDTO } from './fact-table-dto';

export class RevisionDTO {
    id: string;
    dataset_id?: string;
    revision_index: number;
    previous_revision_id?: string;
    online_cube_filename?: string;
    factTables: FactTableDTO[];
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
        revDto.online_cube_filename = revision.onlineCubeFilename;
        revDto.publish_at = revision.publishAt?.toISOString();
        revDto.approved_at = revision.approvedAt?.toISOString();
        revDto.approved_by = revision.approvedBy?.name;
        revDto.created_by = revision.createdBy?.name;
        revDto.factTables = revision.factTables?.map((factTable: FactTable) => FactTableDTO.fromFactTable(factTable));
        return revDto;
    }
}
