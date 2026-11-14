import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const exec = promisify(execFile);

export function isolatedCompose(
  project: string,
  ports: { redis: number; postgres: number },
) {
  if (!/^reliq-chaos-[0-9a-f]+$/.test(project))
    throw new Error('Expected an isolated chaos project name');
  const env = {
    ...process.env,
    REDIS_HOST_PORT: String(ports.redis),
    POSTGRES_HOST_PORT: String(ports.postgres),
  };
  const docker = async (...args: string[]) =>
    (await exec('docker', args, { env, timeout: 120_000 })).stdout.trim();
  const compose = (...args: string[]) =>
    docker('compose', '-p', project, ...args);

  return {
    start: () => compose('up', '-d', '--wait', 'redis', 'postgres'),
    stop: () => compose('down', '-v'),
    async urls() {
      const [redis, postgres] = await Promise.all([
        compose('port', 'redis', '6379'),
        compose('port', 'postgres', '5432'),
      ]);
      return {
        redis: `redis://${redis}/0`,
        postgres: `postgresql://reliq:reliq@${postgres}/reliq`,
      };
    },
    async redisContainer() {
      const id = await compose('ps', '-q', 'redis');
      if (!/^[0-9a-f]{12,64}$/.test(id))
        throw new Error('Could not identify this project’s Redis container');
      const labels = await docker(
        'inspect',
        '--format',
        '{{index .Config.Labels "com.docker.compose.project"}}/{{index .Config.Labels "com.docker.compose.service"}}',
        id,
      );
      if (labels !== `${project}/redis`)
        throw new Error(
          'Redis container does not belong to this chaos project',
        );
      return {
        restart: () => docker('restart', id),
        kill: () => docker('kill', '--signal', 'SIGKILL', id),
        start: () => compose('up', '-d', '--wait', 'redis'),
      };
    },
  };
}
