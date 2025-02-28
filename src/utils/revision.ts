import { Revision } from '../entities/dataset/revision';

export const isPublished = (rev: Revision) => {
    const now = new Date();
    return rev.approvedAt && rev.publishAt && rev.approvedAt < now && rev.publishAt < now;
};
