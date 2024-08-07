export interface DatafileDTO {
    id: string;
    sha256hash: string;
    created_by: string;
    draft: boolean;
    creation_date: Date;
    csv_link: string;
}
