export enum DimensionType {
  Raw = 'raw',
  Text = 'text',
  Numeric = 'numeric',
  Symbol = 'symbol',
  LookupTable = 'lookup_table',
  DatePeriod = 'date_period',
  Date = 'date',
  TimePeriod = 'time_period',
  Time = 'time',
  NoteCodes = 'note_codes'
}

export const DateDimensionTypes = [
  DimensionType.DatePeriod,
  DimensionType.Date,
  DimensionType.TimePeriod,
  DimensionType.Time
];

export const LookupTableTypes = [...DateDimensionTypes, DimensionType.LookupTable];
