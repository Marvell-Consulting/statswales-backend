export interface ResultsetWithCount<T> {
    data: T[]; // paginated data
    count: number; // total number of records without pagination
}
