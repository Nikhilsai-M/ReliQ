import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  cpSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join, resolve } from 'node:path';
import { z } from 'zod';

const dist = resolve('dist');
const scratch = mkdtempSync(join(dist, 'idempotency-mutation-'));
const seed = process.argv[2] ?? '20261115';
const jobs = process.argv[3] ?? '100';
const source = join(scratch, 'dist/src/jobs/transfer.js');

function run(label: string) {
  const log = spawnSync(
    process.execPath,
    ['dist/scripts/chaos/run.js', 'graceful', seed, jobs],
    {
      cwd: scratch,
      encoding: 'utf8',
      timeout: 180_000,
    },
  );
  console.log(log.stdout);
  if (log.stderr) console.error(log.stderr);
  if (log.error) throw log.error;
  const dir = join(scratch, 'chaos-results');
  const runDir = readdirSync(dir, { withFileTypes: true }).find((entry) =>
    entry.isDirectory(),
  )?.name;
  assert(runDir, 'Chaos run did not produce a report');
  const report = z
    .object({ duplicated_effects: z.int().nonnegative() })
    .parse(JSON.parse(readFileSync(join(dir, runDir, 'summary.json'), 'utf8')));
  cpSync(join(dir, runDir), resolve('chaos-results', `${runDir}-${label}`), {
    recursive: true,
  });
  rmSync(join(dir, runDir), { recursive: true });
  return { status: log.status, stderr: log.stderr, ...report };
}

try {
  const compiled = join(scratch, 'dist');
  cpSync(join(dist, 'src'), join(compiled, 'src'), { recursive: true });
  cpSync(join(dist, 'scripts'), join(compiled, 'scripts'), { recursive: true });
  cpSync('src/db/migrations', join(scratch, 'src/db/migrations'), {
    recursive: true,
  });
  cpSync('compose.yaml', join(scratch, 'compose.yaml'));
  cpSync('package.json', join(scratch, 'package.json'));
  const original = readFileSync(source, 'utf8');
  // Skipping the key insertion and row-count gate models a handler without its idempotency guard.
  const guard =
    /const inserted = await client\.query\(`INSERT INTO idempotency_keys[\s\S]*?if \(inserted\.rowCount === 1\) \{/;
  assert.match(original, guard);
  writeFileSync(source, original.replace(guard, '{'));
  console.log(
    'Mutation enabled: bypassed transfer step idempotency key and ON CONFLICT gate',
  );
  const mutated = run('mutated');
  assert.equal(mutated.status, 1);
  assert.match(mutated.stderr, /Duplicate committed side effects/);
  assert(
    mutated.duplicated_effects > 0,
    'The run must detect a duplicate effect',
  );
  writeFileSync(source, original);
  console.log('Restored transfer idempotency guard');
  const restored = run('restored');
  assert.equal(restored.status, 0);
  assert.equal(restored.duplicated_effects, 0);
} finally {
  assert.equal(resolve(scratch, '..'), dist);
  rmSync(scratch, { recursive: true });
}
