import { Revision } from '../entities/dataset/revision';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { Designation } from '../enums/designation';
import { RevisionTask } from '../interfaces/revision-task';

import { DataTableDto } from './data-table-dto';
import { RelatedLinkDTO } from './related-link-dto';
import { RevisionProviderDTO } from './revision-provider-dto';
import { RevisionMetadataDTO } from './revistion-metadata-dto';
import { TopicDTO } from './topic-dto';
import { UpdateFrequencyDTO } from './update-frequency-dto';

export class RevisionDTO {
    id: string;
    dataset_id?: string;
    revision_index: number;
    previous_revision_id?: string;
    online_cube_filename?: string;
    data_table_id?: string;
    data_table?: DataTableDto;
    created_at: string;
    created_by: string;
    updated_at: string;
    approved_at?: string;
    approved_by?: string;
    publish_at?: string;
    metadata?: RevisionMetadataDTO[];
    rounding_applied?: boolean;
    update_frequency?: UpdateFrequencyDTO;
    designation?: Designation;
    related_links?: RelatedLinkDTO[];
    providers?: RevisionProviderDTO[];
    topics?: TopicDTO[];

    tasks?: RevisionTask;

    static fromRevision(revision: Revision): RevisionDTO {
        const revDto = new RevisionDTO();
        revDto.id = revision.id;
        revDto.revision_index = revision.revisionIndex;
        revDto.dataset_id = revision.dataset?.id;
        revDto.data_table_id = revision.dataTableId;
        revDto.created_at = revision.createdAt.toISOString();
        revDto.updated_at = revision.updatedAt.toISOString();
        revDto.previous_revision_id = revision.previousRevisionId;
        revDto.online_cube_filename = revision.onlineCubeFilename || undefined;
        revDto.publish_at = revision.publishAt?.toISOString();
        revDto.approved_at = revision.approvedAt?.toISOString();
        revDto.approved_by = revision.approvedBy?.name;
        revDto.created_by = revision.createdBy?.name;

        if (revision.dataTable) {
            revDto.data_table = DataTableDto.fromDataTable(revision.dataTable);
        }

        revDto.rounding_applied = revision.roundingApplied;
        revDto.update_frequency = UpdateFrequencyDTO.fromDuration(revision.updateFrequency);
        revDto.designation = revision.designation;
        revDto.related_links = revision.relatedLinks?.map((relLink) => RelatedLinkDTO.fromRelatedLink(relLink));

        if (revision.metadata) {
            revDto.metadata = revision.metadata.map((meta) => RevisionMetadataDTO.fromRevisionMetadata(meta));
        }

        revDto.providers = revision.revisionProviders?.map((revProvider: RevisionProvider) =>
            RevisionProviderDTO.fromRevisionProvider(revProvider)
        );

        revDto.topics = revision.revisionTopics?.map((revTopic: RevisionTopic) => TopicDTO.fromTopic(revTopic.topic));

        return revDto;
    }
}
