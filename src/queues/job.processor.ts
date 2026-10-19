import { Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { PrismaService } from '../database/prisma.service';

export interface ProcessJobPayload {
  jobRecordId: string;
  tenantId: string;
  payload: Record<string, any>;
}

@Processor('data-processing-queue', {
  concurrency: 5,
})
export class JobProcessor extends WorkerHost {
  private readonly logger = new Logger(JobProcessor.name);

  constructor(private readonly prisma: PrismaService) {
    super();
  }

  async process(job: Job<ProcessJobPayload>): Promise<any> {
    const { jobRecordId, tenantId, payload } = job.data;
    this.logger.log(`[Job ${job.id}] Processing started for Tenant: ${tenantId}`);

    await this.prisma.jobRecord.update({
      where: { id: jobRecordId },
      data: {
        status: 'PROCESSING',
        attempts: job.attemptsMade + 1,
      },
    });

    const result = await this.executeHeavyTask(payload);

    await this.prisma.jobRecord.update({
      where: { id: jobRecordId },
      data: {
        status: 'COMPLETED',
        result: result,
        processedAt: new Date(),
      },
    });

    this.logger.log(`[Job ${job.id}] Completed successfully`);
    return result;
  }

  private async executeHeavyTask(payload: any): Promise<any> {
    // Simulated processing delay
    await new Promise((resolve) => setTimeout(resolve, 1500));

    if (payload?.simulateFailure) {
      throw new Error('Downstream microservice connection timeout!');
    }

    return { processedRecords: 250, executionTimestamp: new Date().toISOString() };
  }

  @OnWorkerEvent('failed')
  async onFailed(job: Job<ProcessJobPayload>, err: Error) {
    this.logger.error(
      `[Job ${job.id}] Attempt ${job.attemptsMade} failed: ${err.message}`,
    );

    if (job.attemptsMade >= (job.opts.attempts || 3)) {
      this.logger.error(
        `[Job ${job.id}] Retries exhausted! Moving to Dead-Letter state.`,
      );

      await this.prisma.jobRecord.update({
        where: { id: job.data.jobRecordId },
        data: {
          status: 'FAILED',
          errorMessage: err.message,
        },
      });
    }
  }
}
