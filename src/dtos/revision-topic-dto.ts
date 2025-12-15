import { RevisionTopic } from '../entities/dataset/revision-topic';

import { TopicDTO } from './topic-dto';
import { Locale } from '../enums/locale';

export class RevisionTopicDTO {
  id: string;
  revision_id: string;
  topic_id: number;
  topic: TopicDTO;

  static fromRevisionTopic(revTopic: RevisionTopic, lang?: Locale): RevisionTopicDTO {
    const dto = new RevisionTopicDTO();
    dto.id = revTopic.id;
    dto.revision_id = revTopic.revisionId;
    dto.topic_id = revTopic.topicId;
    dto.topic = TopicDTO.fromTopic(revTopic.topic, lang);

    return dto;
  }
}
