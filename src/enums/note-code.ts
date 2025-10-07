import { NoteCodeItem } from '../interfaces/note-code-item';

export enum NoteCode {
  Average = 'a',
  BreakInSeries = 'b',
  Confidential = 'c',
  Estimated = 'e',
  Forecast = 'f',
  LowFigure = 'k',
  LowReliability = 'u',
  MissingData = 'x',
  NotApplicable = 'z',
  NotRecorded = 'w',
  NotStatisticallySignificant = 'ns',
  Provisional = 'p',
  Revised = 'r',
  StatisticallySignificantL1 = 's',
  StatisticallySignificantL2 = 'ss',
  StatisticallySignificantL3 = 'sss',
  Total = 't'
}

export const NoteCodes: NoteCodeItem[] = [
  { code: NoteCode.Average, tag: 'average' },
  { code: NoteCode.BreakInSeries, tag: 'break_in_series' },
  { code: NoteCode.Confidential, tag: 'confidential' },
  { code: NoteCode.Estimated, tag: 'estimated' },
  { code: NoteCode.Forecast, tag: 'forecast' },
  { code: NoteCode.LowFigure, tag: 'low_figure' },
  { code: NoteCode.NotStatisticallySignificant, tag: 'not_statistically_significant' },
  { code: NoteCode.Provisional, tag: 'provisional' },
  { code: NoteCode.Revised, tag: 'revised' },
  { code: NoteCode.StatisticallySignificantL1, tag: 'statistically_significant_at_level_1' },
  { code: NoteCode.StatisticallySignificantL2, tag: 'statistically_significant_at_level_2' },
  { code: NoteCode.StatisticallySignificantL3, tag: 'statistically_significant_at_level_3' },
  { code: NoteCode.Total, tag: 'total' },
  { code: NoteCode.LowReliability, tag: 'low_reliability' },
  { code: NoteCode.NotRecorded, tag: 'not_recorded' },
  { code: NoteCode.MissingData, tag: 'missing_data' },
  { code: NoteCode.NotApplicable, tag: 'not_applicable' }
];
