import { ConsumerRevisionDTO } from '../dtos/consumer-revision-dto';
import { Revision } from '../entities/dataset/revision';

export const isPublished = (rev: Revision | ConsumerRevisionDTO): boolean => {
  const now = new Date();
  if (rev instanceof ConsumerRevisionDTO) {
    return !!(rev.approved_at && rev.publish_at && new Date(rev.approved_at) < now && new Date(rev.publish_at) < now);
  }
  return !!(rev.approvedAt && rev.publishAt && rev.approvedAt < now && rev.publishAt < now);
};
