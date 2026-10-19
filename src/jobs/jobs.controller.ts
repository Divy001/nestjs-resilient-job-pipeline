import {
  Controller,
  Post,
  Get,
  Body,
  Headers,
  Param,
  HttpCode,
  HttpStatus,
  UseGuards,
  NotFoundException,
} from '@nestjs/common';
import { JobsService } from './jobs.service';
import { IngestJobDto } from './dto/ingest-job.dto';
import { IdempotencyGuard } from '../common/guards/idempotency.guard';

@Controller('api/v1/jobs')
export class JobsController {
  constructor(private readonly jobsService: JobsService) {}

  @Post('ingest')
  @HttpCode(HttpStatus.ACCEPTED)
  @UseGuards(IdempotencyGuard)
  async ingest(
    @Headers('x-idempotency-key') idempotencyKey: string,
    @Headers('x-tenant-id') tenantId: string = 'default-tenant',
    @Body() dto: IngestJobDto,
  ) {
    return this.jobsService.ingestJob(tenantId, idempotencyKey, dto);
  }

  @Get(':id/status')
  async getStatus(
    @Param('id') jobId: string,
    @Headers('x-tenant-id') tenantId: string = 'default-tenant',
  ) {
    const job = await this.jobsService.getJobStatus(jobId, tenantId);
    if (!job) {
      throw new NotFoundException(`Job with ID ${jobId} not found`);
    }
    return job;
  }
}
