import { Revision } from '../entities/dataset/revision';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { Designation } from '../enums/designation';
import { RelatedLinkDTO } from './related-link-dto';
import { RevisionProviderDTO } from './revision-provider-dto';
import { RevisionMetadataDTO } from './revistion-metadata-dto';
import { TopicDTO } from './topic-dto';
import { UpdateFrequencyDTO } from './update-frequency-dto';

export class ConsumerRevisionDTO {
  id: string;
  dataset_id?: string;
  revision_index: number;
  previous_revision_id?: string;
  created_at: string;
  updated_at: string;
  approved_at?: string;
  publish_at?: string;
  metadata?: RevisionMetadataDTO[];
  rounding_applied?: boolean;
  update_frequency?: UpdateFrequencyDTO;
  designation?: Designation;
  related_links?: RelatedLinkDTO[];
  providers?: RevisionProviderDTO[];
  topics?: TopicDTO[];

  static fromRevision(revision: Revision): ConsumerRevisionDTO {
    const revDto = new ConsumerRevisionDTO();
    revDto.id = revision.id;
    revDto.revision_index = revision.revisionIndex;
    revDto.dataset_id = revision.dataset?.id;
    revDto.created_at = revision.createdAt.toISOString();
    revDto.updated_at = revision.updatedAt.toISOString();
    revDto.previous_revision_id = revision.previousRevisionId;
    revDto.publish_at = revision.publishAt?.toISOString();
    revDto.approved_at = revision.approvedAt?.toISOString();

    revDto.rounding_applied = revision.roundingApplied;
    revDto.update_frequency = revision.updateFrequency;
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
