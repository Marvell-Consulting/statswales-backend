export interface DimensionUpdateTask {
    id: string;
    lookupTableUpdated: boolean;
}

export interface RevisionTask {
    dimensions: DimensionUpdateTask[];
    measure?: DimensionUpdateTask;
}
