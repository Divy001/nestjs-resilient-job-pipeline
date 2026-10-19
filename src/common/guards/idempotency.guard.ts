import {
  Injectable,
  CanActivate,
  ExecutionContext,
  BadRequestException,
  ConflictException,
  Inject,
} from '@nestjs/common';
import { Request } from 'express';
import Redis from 'ioredis';

@Injectable()
export class IdempotencyGuard implements CanActivate {
  constructor(@Inject('REDIS_CLIENT') private readonly redis: Redis) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<Request>();

    const idempotencyKey = request.headers['x-idempotency-key'] as string;
    if (!idempotencyKey) {
      throw new BadRequestException(
        'Missing required header: X-Idempotency-Key',
      );
    }

    const tenantId = (request.headers['x-tenant-id'] as string) || 'default';
    const redisLockKey = `idempotency:${tenantId}:${idempotencyKey}`;

    // Atomic SETNX with 60 second TTL
    const lockAcquired = await this.redis.set(
      redisLockKey,
      'IN_PROGRESS',
      'EX',
      60,
      'NX',
    );

    if (!lockAcquired) {
      const currentStatus = await this.redis.get(redisLockKey);

      if (currentStatus === 'IN_PROGRESS') {
        throw new ConflictException(
          'A request with this idempotency key is already actively processing. Please wait.',
        );
      }

      throw new ConflictException(
        'Duplicate request: This operation has already been submitted and processed.',
      );
    }

    return true;
  }
}
