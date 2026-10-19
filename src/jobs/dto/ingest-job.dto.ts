import { IsNotEmpty, IsString, IsObject } from 'class-validator';

export class IngestJobDto {
  @IsString()
  @IsNotEmpty()
  jobType: string;

  @IsObject()
  @IsNotEmpty()
  payload: Record<string, any>;
}
