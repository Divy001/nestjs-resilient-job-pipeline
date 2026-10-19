import {
  Injectable,
  InternalServerErrorException,
  Logger,
  Inject,
} from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { PrismaService } from '../database/prisma.service';
import { IngestJobDto } from './dto/ingest-job.dto';

@Injectable()
export class JobsService {
  private readonly logger = new Logger(JobsService.name);

  constructor(
    private readonly prisma: PrismaService,
    @InjectQueue('data-processing-queue') private readonly jobQueue: Queue,
    @Inject('REDIS_CLIENT') private readonly redis: Redis,
  ) {}

  async ingestJob(tenantId: string, idempotencyKey: string, dto: IngestJobDto) {
    let jobRecord;
    try {
      // Step 1: Create Single Source of Truth record in PostgreSQL
      jobRecord = await this.prisma.jobRecord.create({
        data: {
          tenantId,
          idempotencyKey,
          jobType: dto.jobType,
          payload: dto.payload,
          status: 'PENDING',
        },
      });

      // Step 2: Push to BullMQ queue with identical ID for unified traceability
      await this.jobQueue.add(
        'process-job',
        {
          jobRecordId: jobRecord.id,
          tenantId,
          payload: dto.payload,
        },
        {
          jobId: jobRecord.id,
        },
      );

      this.logger.log(`Job queued successfully. ID: ${jobRecord.id}`);

      return {
        jobId: jobRecord.id,
        status: jobRecord.status,
        trackingUrl: `/api/v1/jobs/${jobRecord.id}/status`,
        createdAt: jobRecord.createdAt,
      };
    } catch (error) {
      this.logger.error(`Failed to ingest job: ${error.message}`);

      // Dual-Write protection: If queueing failed, don't leave ghost PENDING record
      if (jobRecord) {
        await this.prisma.jobRecord.update({
          where: { id: jobRecord.id },
          data: {
            status: 'FAILED',
            errorMessage: `Queue dispatch failure: ${error.message}`,
          },
        });
      }

      // Release idempotency lock so client can retry
      const redisLockKey = `idempotency:${tenantId}:${idempotencyKey}`;
      await this.redis.del(redisLockKey);

      throw new InternalServerErrorException(
        'Failed to queue background job. Please retry.',
      );
    }
  }

  async getJobStatus(jobId: string, tenantId: string) {
    return this.prisma.jobRecord.findFirst({
      where: { id: jobId, tenantId },
      select: {
        id: true,
        tenantId: true,
        jobType: true,
        status: true,
        attempts: true,
        result: true,
        errorMessage: true,
        createdAt: true,
        processedAt: true,
      },
    });
  }
}
