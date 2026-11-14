import express from 'express';
import type { Logger } from 'pino';
import { z } from 'zod';
import type { Queue } from '../queue/queue.js';
import {
  enqueueInput,
  promoteLimit,
  queueName,
  replaySelection,
  timestamp,
} from '../queue/schemas.js';
import {
  describeError,
  errorBody,
  handleErrors,
  QueueNotFoundError,
} from './errors.js';
import type { createHealthCheck } from './health.js';

const pagination = z
  .object({
    offset: z
      .string()
      .regex(/^\d+$/)
      .transform(Number)
      .pipe(timestamp)
      .default(0),
    limit: z
      .string()
      .regex(/^\d+$/)
      .transform(Number)
      .pipe(promoteLimit)
      .default(50),
  })
  .strict();
const generation = replaySelection.pick({ replayCount: true }).strict();
const bulk = z
  .object({ jobs: z.array(replaySelection.strict()).min(1).max(100) })
  .strict();

export function createApp(
  getQueue: (name: string) => Queue | undefined,
  logger: Logger,
  health: ReturnType<typeof createHealthCheck>,
) {
  const app = express();
  app.disable('x-powered-by');
  const queues = express.Router();
  const readQueue = (value: unknown) => {
    const name = queueName.parse(value);
    const queue = getQueue(name);
    if (!queue) throw new QueueNotFoundError(name);
    return queue;
  };

  app.get('/health', async (_request, response) => {
    response.set('Cache-Control', 'no-store');
    const services = await health();
    if (services.redis === 'up' && services.postgres === 'up') {
      response.json({ status: 'ok', services });
    } else {
      response
        .status(503)
        .json({
          error: errorBody('UNHEALTHY', 'A dependency is unavailable', [
            services,
          ]),
        });
    }
  });

  queues.use((_request, response, next) => {
    response.set('Cache-Control', 'no-store');
    next();
  });
  queues.get('/:queue/stats', async (request, response) => {
    const queue = readQueue(request.params.queue);
    response.json({ queue: request.params.queue, depth: await queue.depth() });
  });
  queues.get('/:queue/in-flight', async (request, response) => {
    const queue = readQueue(request.params.queue);
    const { offset, limit } = pagination.parse(request.query);
    response.json({
      offset,
      limit,
      jobs: await queue.listInFlight(offset, limit),
    });
  });
  queues.get('/:queue/dlq', async (request, response) => {
    const queue = readQueue(request.params.queue);
    const { offset, limit } = pagination.parse(request.query);
    response.json({ offset, limit, jobs: await queue.listDead(offset, limit) });
  });

  queues.use(
    (request, response, next) => {
      if (
        ['POST', 'DELETE'].includes(request.method) &&
        !request.is('application/json')
      ) {
        response.status(415).json({
          error: errorBody('UNSUPPORTED_MEDIA_TYPE', 'Use application/json'),
        });
        return;
      }
      next();
    },
    express.json({ limit: '256kb' }),
  );

  queues.post('/:queue/jobs', async (request, response) => {
    const queue = readQueue(request.params.queue);
    response
      .status(201)
      .json(await queue.enqueue(enqueueInput.parse(request.body)));
  });
  queues.post('/:queue/dlq/replay', async (request, response) => {
    const queue = readQueue(request.params.queue);
    const { jobs } = bulk.parse(request.body);
    const results = await queue.replayMany(jobs);
    response.json({
      results: results.map((entry) => {
        if (entry.status === 'replayed') return entry;
        const { status, body } = describeError(entry.error);
        if (status === 500)
          logger.error(
            { err: entry.error, jobId: entry.id },
            'Bulk replay item failed',
          );
        return { id: entry.id, status: entry.status, error: body };
      }),
    });
  });
  queues.post('/:queue/dlq/:id/replay', async (request, response) => {
    const queue = readQueue(request.params.queue);
    const selection = replaySelection.parse({
      id: request.params.id,
      ...generation.parse(request.body),
    });
    response.json(await queue.replay(selection));
  });
  queues.delete('/:queue/dlq/:id', async (request, response) => {
    const queue = readQueue(request.params.queue);
    await queue.purge(
      replaySelection.parse({
        id: request.params.id,
        ...generation.parse(request.body),
      }),
    );
    response.status(204).end();
  });

  app.use('/queues', queues);
  app.use((_request, response) => {
    response
      .status(404)
      .json({ error: errorBody('ROUTE_NOT_FOUND', 'Route not found') });
  });
  app.use(handleErrors(logger));
  return app;
}
