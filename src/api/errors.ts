import type { ErrorRequestHandler } from 'express';
import type { Logger } from 'pino';
import { ZodError } from 'zod';
import {
  DeadGenerationConflictError,
  JobExistsError,
  JobNotFoundError,
} from '../queue/errors.js';

export function errorBody(
  code: string,
  message: string,
  details: unknown[] = [],
) {
  return { code, message, details };
}

export class QueueNotFoundError extends Error {
  constructor(name: string) {
    super(`Queue not found: ${name}`);
    this.name = 'QueueNotFoundError';
  }
}

export function describeError(error: unknown) {
  if (error instanceof URIError) {
    return {
      status: 400,
      body: errorBody('VALIDATION_ERROR', 'Invalid request path encoding'),
    };
  }
  if (error instanceof ZodError) {
    return {
      status: 400,
      body: errorBody(
        'VALIDATION_ERROR',
        'Invalid request',
        error.issues.map(({ path, message }) => ({ path, message })),
      ),
    };
  }
  if (error instanceof QueueNotFoundError)
    return { status: 404, body: errorBody('QUEUE_NOT_FOUND', error.message) };
  if (error instanceof JobNotFoundError)
    return {
      status: 404,
      body: errorBody('JOB_NOT_FOUND', error.message),
    };
  if (error instanceof JobExistsError)
    return {
      status: 409,
      body: errorBody('JOB_EXISTS', error.message),
    };
  if (error instanceof DeadGenerationConflictError)
    return {
      status: 409,
      body: errorBody('DEAD_GENERATION_CONFLICT', error.message),
    };
  if (error instanceof RangeError)
    return {
      status: 400,
      body: errorBody('VALIDATION_ERROR', error.message),
    };
  return {
    status: 500,
    body: errorBody('INTERNAL_ERROR', 'Queue operation failed'),
  };
}

export function handleErrors(logger: Logger): ErrorRequestHandler {
  return (error: unknown, _request, response, next) => {
    if (response.headersSent) return next(error);
    const parseError =
      error instanceof Error && 'type' in error ? error.type : undefined;
    if (parseError === 'entity.parse.failed') {
      response.status(400).json({
        error: errorBody('INVALID_JSON', 'Malformed JSON body'),
      });
    } else if (parseError === 'entity.too.large') {
      response.status(413).json({
        error: errorBody('BODY_TOO_LARGE', 'JSON body exceeds 256 KiB'),
      });
    } else if (
      parseError === 'charset.unsupported' ||
      parseError === 'encoding.unsupported'
    ) {
      response.status(415).json({
        error: errorBody('UNSUPPORTED_MEDIA_TYPE', 'Unsupported JSON encoding'),
      });
    } else {
      const { status, body } = describeError(error);
      if (status === 500) logger.error({ err: error }, 'Queue request failed');
      response.status(status).json({ error: body });
    }
  };
}
