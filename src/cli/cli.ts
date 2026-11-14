import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { z } from 'zod';
import { loadApiPort } from '../config/api.js';
import { queueName } from '../queue/schemas.js';

const usage = `Usage: npm run cli -- <command> <queue> [options]

  stats <queue>
  in-flight <queue> [--offset <n>] [--limit <n>]
  dlq <queue> [--offset <n>] [--limit <n>]
  enqueue <queue> --file <job.json>
  replay <queue> --id <id> --generation <n>
  replay-bulk <queue> --file <jobs.json>
  purge <queue> --id <id> --generation <n>

All commands accept --url <api-base-url>. The default is http://127.0.0.1:3000 (or API_PORT).`;

const integer = z
  .string()
  .regex(/^\d+$/)
  .transform(Number)
  .pipe(z.int().nonnegative());
const idSchema = z.uuid();
const commands = z.enum([
  'stats',
  'in-flight',
  'dlq',
  'enqueue',
  'replay',
  'replay-bulk',
  'purge',
]);
const optionsByCommand = {
  stats: [],
  'in-flight': ['offset', 'limit'],
  dlq: ['offset', 'limit'],
  enqueue: ['file'],
  replay: ['id', 'generation'],
  'replay-bulk': ['file'],
  purge: ['id', 'generation'],
};

async function run(args: string[]) {
  const { positionals, values } = parseArgs({
    args,
    allowPositionals: true,
    options: {
      help: { type: 'boolean', short: 'h' },
      url: { type: 'string' },
      file: { type: 'string' },
      id: { type: 'string' },
      generation: { type: 'string' },
      offset: { type: 'string' },
      limit: { type: 'string' },
    },
  });
  if (values.help || positionals.length === 0) {
    console.log(usage);
    return;
  }

  const [command, queue] = z.tuple([commands, queueName]).parse(positionals);
  const allowed: readonly string[] = optionsByCommand[command];
  for (const option of Object.keys(values)) {
    if (option !== 'url' && !allowed.includes(option))
      throw new Error(`Unexpected option --${option} for ${command}`);
  }
  const baseUrl = values.url ?? `http://127.0.0.1:${loadApiPort()}`;
  const url = new URL(`/queues/${encodeURIComponent(queue)}`, baseUrl);
  let method = 'GET';
  let body: string | undefined;

  switch (command) {
    case 'stats':
    case 'in-flight':
    case 'dlq':
      url.pathname += `/${command}`;
      if (values.limit !== undefined)
        url.searchParams.set('limit', String(integer.parse(values.limit)));
      if (values.offset !== undefined)
        url.searchParams.set('offset', String(integer.parse(values.offset)));
      break;
    case 'enqueue':
    case 'replay-bulk':
      method = 'POST';
      url.pathname += command === 'enqueue' ? '/jobs' : '/dlq/replay';
      body = await readFile(
        z.string().min(1, 'Use --file with a JSON document').parse(values.file),
        'utf8',
      );
      break;
    case 'replay':
    case 'purge':
      method = command === 'replay' ? 'POST' : 'DELETE';
      url.pathname += `/dlq/${idSchema.parse(values.id)}${command === 'replay' ? '/replay' : ''}`;
      body = JSON.stringify({ replayCount: integer.parse(values.generation) });
      break;
  }

  const response = await fetch(url, {
    method,
    signal: AbortSignal.timeout(10_000),
    ...(body === undefined
      ? {}
      : {
          body,
          headers: { 'content-type': 'application/json' },
        }),
  });
  if (response.status === 204) {
    console.log(JSON.stringify({ purged: values.id }));
    return;
  }
  const result: unknown = await response.json();
  const output = JSON.stringify(result, null, 2);
  if (!response.ok) {
    process.exitCode = 1;
    console.error(output);
    return;
  }
  const batch = z
    .object({ results: z.array(z.object({ status: z.string() })) })
    .safeParse(result);
  if (
    command === 'replay-bulk' &&
    batch.success &&
    batch.data.results.some(({ status }) => status === 'failed')
  )
    process.exitCode = 1;
  console.log(output);
}

try {
  await run(process.argv.slice(2));
} catch (error) {
  const message = error instanceof Error ? error.message : 'CLI request failed';
  console.error(JSON.stringify({ error: { code: 'CLI_ERROR', message } }));
  process.exitCode = 1;
}
