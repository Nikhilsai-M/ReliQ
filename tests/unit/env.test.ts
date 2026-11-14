import { describe, expect, it } from 'vitest';
import { ConfigError, loadConfig } from '../../src/config/env.js';

const env = {
  REDIS_URL: 'redis://127.0.0.1:16379/0',
  DATABASE_URL: 'postgresql://reliq:reliq@127.0.0.1:15432/reliq',
};

describe('configuration', () => {
  it('validates the connection URLs and supplies a default log level', () => {
    expect(loadConfig(env)).toEqual({ ...env, LOG_LEVEL: 'info' });
  });

  it('reports missing settings', () => {
    expect(() => loadConfig({})).toThrow(
      'Invalid environment variables: REDIS_URL, DATABASE_URL',
    );
  });

  it('rejects unsupported protocols and log levels without exposing the URL', () => {
    const invalid = {
      ...env,
      REDIS_URL: 'https://user:secret@example.com',
      LOG_LEVEL: 'verbose',
    };
    expect(() => loadConfig(invalid)).toThrow(ConfigError);
    expect(() => loadConfig(invalid)).toThrow(
      /^Invalid environment variables: REDIS_URL, LOG_LEVEL$/,
    );
  });
});
