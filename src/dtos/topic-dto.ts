import { Topic } from '../entities/dataset/topic';
import { Locale } from '../enums/locale';

export class TopicDTO {
    id: number;
    path: string;
    name: string;

    static fromTopic(topic: Topic, lang: Locale): TopicDTO {
        const dto = new TopicDTO();
        dto.id = topic.id;
        dto.path = topic.path;
        dto.name = lang.includes('en') ? topic.nameEN : topic.nameCY;
        return dto;
    }
}
