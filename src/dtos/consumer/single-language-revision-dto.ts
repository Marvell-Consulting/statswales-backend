import { Locale } from '../../enums/locale';
import { Designation } from '../../enums/designation';
import { RevisionMetadataDTO } from '../revistion-metadata-dto';
import { UpdateFrequencyDTO } from '../update-frequency-dto';
import { RevisionProviderDTO } from '../revision-provider-dto';
import { RevisionProvider } from '../../entities/dataset/revision-provider';
import { RevisionTopic } from '../../entities/dataset/revision-topic';
import { RevisionMetadata } from '../../entities/dataset/revision-metadata';
import { Revision } from '../../entities/dataset/revision';
import { RelatedLink, RelatedLinkDTO } from '../related-link-dto';
import { SingleLanguageTopicDTO } from './single-language-topic-dto';

export class SingleLanguageRevisionDTO {
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
  topics?: SingleLanguageTopicDTO[];
  coverage_start_date?: string;
  coverage_end_date?: string;

  static fromRevision(revision: Revision, lang: Locale): SingleLanguageRevisionDTO {
    const revDto = new SingleLanguageRevisionDTO();
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

    revDto.related_links = revision.relatedLinks?.map((link: RelatedLink) => RelatedLinkDTO.fromRelatedLink(link));

    revDto.providers = revision.revisionProviders
      ?.filter((provider: RevisionProvider) => provider.language.includes(lang))
      .map((provider: RevisionProvider) => RevisionProviderDTO.fromRevisionProvider(provider));

    revDto.topics = revision.revisionTopics?.map((revTopic: RevisionTopic) =>
      SingleLanguageTopicDTO.fromTopic(revTopic.topic, lang)
    );

    const metadata = revision.metadata?.find((meta: RevisionMetadata) => meta.language === lang);
    if (metadata) {
      revDto.metadata = RevisionMetadataDTO.fromRevisionMetadata(metadata);
    }

    return revDto;
  }
}
