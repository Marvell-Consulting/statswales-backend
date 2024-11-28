import { ArrayMinSize, IsArray, IsNotEmpty, IsNumberString } from 'class-validator';

export class TopicSelectionDTO {
    @IsArray()
    @ArrayMinSize(1)
    @IsNotEmpty({ each: true })
    @IsNumberString({ no_symbols: true }, { each: true })
    topics: string[];
}
