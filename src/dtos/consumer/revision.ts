import { RevisionMetadataDTO } from '../revistion-metadata-dto';
import { UpdateFrequencyDTO } from '../update-frequency-dto';
import { Designation } from '../../enums/designation';
import { RelatedLinkDTO } from '../related-link-dto';
import { RevisionProviderDTO } from '../revision-provider-dto';
import { Revision } from '../../entities/dataset/revision';
import { RevisionTopicDTO } from '../revision-topic-dto';

export interface LiteRevision {
  id: string;
  revision_index: number;
  previous_revision_id?: string;
  updated_at: string;
  publish_at?: string;
  coverage_start_date?: string;
  coverage_end_date?: string;
}

export class FullRevision {
  id: string;
  revision_index: number;
  previous_revision_id?: string;
  updated_at: string;
  publish_at?: string;
  metadata?: RevisionMetadataDTO;
  rounding_applied?: boolean;
  update_frequency?: UpdateFrequencyDTO;
  designation?: Designation;
  related_links?: RelatedLinkDTO[];
  providers?: RevisionProviderDTO[];
  topics?: RevisionTopicDTO[];
  coverage_start_date?: string;
  coverage_end_date?: string;

  static fromRevision(revision: Revision, language: string): FullRevision {
    const revDto = new FullRevision();
    revDto.id = revision.id;
    revDto.revision_index = revision.revisionIndex;
    revDto.previous_revision_id = revision.previousRevisionId;
    revDto.updated_at = revision.updatedAt.toISOString();
    revDto.publish_at = revision.publishAt?.toISOString();
    revDto.rounding_applied = revision.roundingApplied;
    revDto.update_frequency = revision.updateFrequency;
    revDto.coverage_start_date = revision.startDate?.toISOString();
    revDto.coverage_end_date = revision.endDate?.toISOString();
    revDto.designation = revision.designation;
    revDto.related_links = revision.relatedLinks?.map((link) => RelatedLinkDTO.fromRelatedLink(link));
    revDto.providers = revision.revisionProviders.map((provider) => RevisionProviderDTO.fromRevisionProvider(provider));
    revDto.topics = revision.revisionTopics.map((topic) => RevisionTopicDTO.fromRevisionTopic(topic));
    const metaData = revision.metadata.find((meta) => meta.language === language);
    if (metaData) {
      revDto.metadata = RevisionMetadataDTO.fromRevisionMetadata(metaData);
    }
    return revDto;
  }
}
