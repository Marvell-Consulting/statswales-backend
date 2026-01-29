jest.mock('../../src/dtos/lookup-table-dto', () => ({
  LookupTableDTO: { fromLookupTable: jest.fn().mockReturnValue({ id: 'lt-stub' }) }
}));

jest.mock('../../src/dtos/dimension-metadata-dto', () => ({
  DimensionMetadataDTO: { fromDimensionMetadata: jest.fn().mockReturnValue({ id: 'meta-stub' }) }
}));

import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionType } from '../../src/enums/dimension-type';
import { DimensionDTO } from '../../src/dtos/dimension-dto';
import { LookupTableDTO } from '../../src/dtos/lookup-table-dto';
import { DimensionMetadataDTO } from '../../src/dtos/dimension-metadata-dto';

describe('DimensionDTO', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const makeDimension = (overrides = {}): Dimension => {
    return {
      id: 'dim-1',
      type: DimensionType.Text,
      extractor: { some: 'config' },
      lookupTable: null,
      joinColumn: 'join_col',
      factTableColumn: 'fact_col',
      isSliceDimension: false,
      metadata: null,
      ...overrides
    } as unknown as Dimension;
  };

  describe('fromDimension', () => {
    it('should map id, type and factTableColumn', () => {
      const dto = DimensionDTO.fromDimension(makeDimension());

      expect(dto.id).toBe('dim-1');
      expect(dto.type).toBe(DimensionType.Text);
      expect(dto.factTableColumn).toBe('fact_col');
    });

    it('should include extractor when present', () => {
      const dto = DimensionDTO.fromDimension(makeDimension({ extractor: { type: 'date' } }));

      expect(dto.extractor).toEqual({ type: 'date' });
    });

    it('should set extractor to undefined when null', () => {
      const dto = DimensionDTO.fromDimension(makeDimension({ extractor: null }));

      expect(dto.extractor).toBeUndefined();
    });

    it('should delegate lookupTable to LookupTableDTO when present', () => {
      const dto = DimensionDTO.fromDimension(makeDimension({ lookupTable: { id: 'lt-1' } }));

      expect(LookupTableDTO.fromLookupTable).toHaveBeenCalledWith({ id: 'lt-1' });
      expect(dto.lookupTable).toEqual({ id: 'lt-stub' });
    });

    it('should set lookupTable to undefined when null', () => {
      const dto = DimensionDTO.fromDimension(makeDimension({ lookupTable: null }));

      expect(dto.lookupTable).toBeUndefined();
    });

    it('should map joinColumn when present and set to undefined when falsy', () => {
      const dto = DimensionDTO.fromDimension(makeDimension({ joinColumn: 'jc' }));
      expect(dto.joinColumn).toBe('jc');

      const dto2 = DimensionDTO.fromDimension(makeDimension({ joinColumn: null }));
      expect(dto2.joinColumn).toBeUndefined();
    });

    it('should delegate metadata mapping to DimensionMetadataDTO', () => {
      const meta = [{ id: 'm1' }, { id: 'm2' }];
      const dto = DimensionDTO.fromDimension(makeDimension({ metadata: meta }));

      expect(DimensionMetadataDTO.fromDimensionMetadata).toHaveBeenCalledTimes(2);
      expect(dto.metadata).toEqual([{ id: 'meta-stub' }, { id: 'meta-stub' }]);
    });

    it('should map isSliceDimension', () => {
      const dto = DimensionDTO.fromDimension(makeDimension({ isSliceDimension: true }));

      expect(dto.isSliceDimension).toBe(true);
    });
  });
});
