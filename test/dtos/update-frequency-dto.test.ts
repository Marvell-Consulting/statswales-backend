import { DurationUnit } from '../../src/enums/duration-unit';
import { UpdateFrequencyDTO } from '../../src/dtos/update-frequency-dto';

describe('UpdateFrequencyDto', () => {
  describe('toDuration', () => {
    test('it returns undefined if the frequency is not defined', () => {
      expect(UpdateFrequencyDTO.toDuration(undefined)).toBe(undefined);
    });

    test('it returns NEVER if the frequency is not updated', () => {
      const updateFrequency: UpdateFrequencyDTO = { is_updated: false };
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('NEVER');
    });

    test('it returns the correct string for days', () => {
      const updateFrequency: UpdateFrequencyDTO = {
        is_updated: true,
        frequency_value: 1,
        frequency_unit: DurationUnit.Day
      };
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P1D');

      updateFrequency.frequency_value = 2;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P2D');

      updateFrequency.frequency_value = 10;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P10D');
    });

    test('it returns the correct string for weeks', () => {
      const updateFrequency: UpdateFrequencyDTO = {
        is_updated: true,
        frequency_value: 1,
        frequency_unit: DurationUnit.Week
      };
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P1W');

      updateFrequency.frequency_value = 4;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P4W');

      updateFrequency.frequency_value = 52;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P52W');
    });

    test('it returns the correct string for months', () => {
      const updateFrequency: UpdateFrequencyDTO = {
        is_updated: true,
        frequency_value: 1,
        frequency_unit: DurationUnit.Month
      };
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P1M');

      updateFrequency.frequency_value = 3;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P3M');

      updateFrequency.frequency_value = 6;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P6M');
    });

    test('it returns the correct string for years', () => {
      const updateFrequency: UpdateFrequencyDTO = {
        is_updated: true,
        frequency_value: 1,
        frequency_unit: DurationUnit.Year
      };
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P1Y');

      updateFrequency.frequency_value = 2;
      expect(UpdateFrequencyDTO.toDuration(updateFrequency)).toBe('P2Y');
    });
  });

  describe('fromDuration', () => {
    test('it returns undefined if the duration is not defined', () => {
      expect(UpdateFrequencyDTO.fromDuration(undefined)).toBe(undefined);
    });

    test('it returns is_updated = false if the duration is NEVER', () => {
      expect(UpdateFrequencyDTO.fromDuration('NEVER')).toEqual({ is_updated: false });
    });

    test('it returns is_updated = true if the duration is P1Y', () => {
      expect(UpdateFrequencyDTO.fromDuration('P1Y')).toEqual({
        is_updated: true,
        frequency_value: 1,
        frequency_unit: DurationUnit.Year
      });
    });

    test('it returns is_updated = true if the duration is P3M', () => {
      expect(UpdateFrequencyDTO.fromDuration('P3M')).toEqual({
        is_updated: true,
        frequency_value: 3,
        frequency_unit: DurationUnit.Month
      });
    });
  });

  describe('in and out', () => {
    test('you get back what you put in', () => {
      const input: UpdateFrequencyDTO = {
        is_updated: true,
        frequency_value: 30,
        frequency_unit: DurationUnit.Day
      };

      const out = UpdateFrequencyDTO.fromDuration(UpdateFrequencyDTO.toDuration(input));

      expect(out).toEqual(input);
    });
  });
});
