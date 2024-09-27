import { Revision } from '../entities/revision';
import { FileImport } from '../entities/file-import';

import { FileImportDTO } from './file-import-dto';

export class RevisionDTO {
    id: string;
    dataset_id?: string;
    revision_index: number;
    previous_revision_id?: string;
    online_cube_filename?: string;
    imports: FileImportDTO[];
    created_at: string;
    created_by: string;
    approved_at?: string;
    approved_by?: string;
    publish_at?: string;

    static async fromRevision(revision: Revision): Promise<RevisionDTO> {
        const revDto = new RevisionDTO();
        revDto.id = revision.id;
        revDto.revision_index = revision.revisionIndex;
        revDto.dataset_id = (await revision.dataset).id;
        revDto.created_at = revision.createdAt.toISOString();
        revDto.previous_revision_id = (await revision.previousRevision)?.id;
        revDto.online_cube_filename = revision.onlineCubeFilename;
        revDto.publish_at = revision.publishAt?.toISOString();
        revDto.approved_at = revision.approvedAt?.toISOString();
        revDto.approved_by = (await revision.approvedBy)?.name || undefined;
        revDto.created_by = (await revision.createdBy).name;
        return revDto;
    }

    static async fromRevisionWithImports(revision: Revision): Promise<RevisionDTO> {
        const revDto = await RevisionDTO.fromRevision(revision);
        revDto.imports = await Promise.all(
            (await revision.imports).map(async (imp: FileImport) => {
                const impDto = await FileImportDTO.fromImport(imp);
                return impDto;
            })
        );
        return revDto;
    }

    static async fromRevisionWithImportsAndSources(revision: Revision): Promise<RevisionDTO> {
        const revDto = await RevisionDTO.fromRevision(revision);
        revDto.imports = await Promise.all(
            (await revision.imports).map(async (imp: FileImport) => {
                const impDto = await FileImportDTO.fromImportWithSources(imp);
                return impDto;
            })
        );
        return revDto;
    }
}
