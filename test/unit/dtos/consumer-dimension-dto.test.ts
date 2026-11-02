import { Dimension } from '../../../src/entities/dataset/dimension';
import { ConsumerDimensionDTO } from '../../../src/dtos/consumer-dimension-dto';
import { DimensionType } from '../../../src/enums/dimension-type';

describe('ConsumerDimensionDTO', () => {
  const makeDimension = (overrides = {}): Dimension => {
    return {
      id: 'dim-1',
      type: DimensionType.LookupTable,
      factTableColumn: 'AreaCode',
      isSliceDimension: false,
      joinColumn: 'reference',
      extractor: { sheet: 'internal' },
      lookupTable: { id: 'lt-1', filename: 'draft-lookup.csv', hash: 'abc' },
      metadata: [
        { language: 'en-GB', name: 'Draft name', description: 'Draft description', notes: 'Draft notes' },
        { language: 'cy-GB', name: 'Enw drafft', description: 'Disgrifiad drafft', notes: 'Nodiadau drafft' }
      ],
      ...overrides
    } as unknown as Dimension;
  };

  const publishedNames = [
    { fact_table_column: 'AreaCode', dimension_name: 'Area', language: 'en-gb' },
    { fact_table_column: 'AreaCode', dimension_name: 'Ardal', language: 'cy-gb' },
    { fact_table_column: 'YearCode', dimension_name: 'Year', language: 'en-gb' }
  ];

  it('should take names from the published cube rather than the dimension metadata', () => {
    const dto = ConsumerDimensionDTO.fromDimension(makeDimension(), publishedNames);

    expect(dto.metadata).toEqual([
      { language: 'en-GB', name: 'Area' },
      { language: 'cy-GB', name: 'Ardal' }
    ]);
  });

  it('should only expose the identifying fields of the dimension', () => {
    const dto = ConsumerDimensionDTO.fromDimension(makeDimension(), publishedNames);

    expect(Object.keys(dto).sort()).toEqual(['factTableColumn', 'id', 'isSliceDimension', 'metadata', 'type']);
    expect(dto).toMatchObject({
      id: 'dim-1',
      type: DimensionType.LookupTable,
      factTableColumn: 'AreaCode',
      isSliceDimension: false
    });
  });

  it('should return no names when the cube has none for the column', () => {
    const dto = ConsumerDimensionDTO.fromDimension(makeDimension({ factTableColumn: 'NoteCode' }), publishedNames);

    expect(dto.metadata).toEqual([]);
  });

  it('should leave metadata undefined when no published names are supplied', () => {
    const dto = ConsumerDimensionDTO.fromDimension(makeDimension());

    expect(dto.metadata).toBeUndefined();
  });
});
