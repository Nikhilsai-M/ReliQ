import type { ErrorRequestHandler } from 'express';
import type { Logger } from 'pino';
import { ZodError } from 'zod';
import {
  DeadGenerationConflictError,
  JobExistsError,
  JobNotFoundError,
} from '../queue/errors.js';

export function describeError(error: unknown) {
  if (error instanceof URIError) {
    return {
      status: 400,
      body: {
        code: 'VALIDATION_ERROR',
        message: 'Invalid request path encoding',
      },
    };
  }
  if (error instanceof ZodError) {
    return {
      status: 400,
      body: {
        code: 'VALIDATION_ERROR',
        message: 'Invalid request',
        details: error.issues.map(({ path, message }) => ({ path, message })),
      },
    };
  }
  if (error instanceof JobNotFoundError)
    return {
      status: 404,
      body: { code: 'JOB_NOT_FOUND', message: error.message },
    };
  if (error instanceof JobExistsError)
    return {
      status: 409,
      body: { code: 'JOB_EXISTS', message: error.message },
    };
  if (error instanceof DeadGenerationConflictError)
    return {
      status: 409,
      body: { code: 'DEAD_GENERATION_CONFLICT', message: error.message },
    };
  if (error instanceof RangeError)
    return {
      status: 400,
      body: { code: 'VALIDATION_ERROR', message: error.message },
    };
  return {
    status: 500,
    body: { code: 'INTERNAL_ERROR', message: 'Queue operation failed' },
  };
}

export function handleErrors(logger: Logger): ErrorRequestHandler {
  return (error: unknown, _request, response, next) => {
    if (response.headersSent) return next(error);
    const parseError =
      error instanceof Error && 'type' in error ? error.type : undefined;
    if (parseError === 'entity.parse.failed') {
      response.status(400).json({
        error: { code: 'INVALID_JSON', message: 'Malformed JSON body' },
      });
    } else if (parseError === 'entity.too.large') {
      response.status(413).json({
        error: {
          code: 'BODY_TOO_LARGE',
          message: 'JSON body exceeds 256 KiB',
        },
      });
    } else if (
      parseError === 'charset.unsupported' ||
      parseError === 'encoding.unsupported'
    ) {
      response.status(415).json({
        error: {
          code: 'UNSUPPORTED_MEDIA_TYPE',
          message: 'Unsupported JSON encoding',
        },
      });
    } else {
      const { status, body } = describeError(error);
      if (status === 500) logger.error({ err: error }, 'Queue request failed');
      response.status(status).json({ error: body });
    }
  };
}
