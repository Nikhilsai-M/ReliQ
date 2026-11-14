import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  cpSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join, resolve } from 'node:path';

// Mutate a disposable source copy without changing the checked-in Lua or the normal build.
const root = resolve('.');
const scratch = mkdtempSync(join(root, 'dist', 'mutation-'));

function execute(test?: string) {
  const result = spawnSync(
    process.execPath,
    [
      `--env-file=${resolve(root, '.env')}`,
      resolve(root, 'node_modules/vitest/vitest.mjs'),
      'run',
      'tests/integration/ownership-mutations.test.ts',
      '--reporter=verbose',
      ...(test ? ['-t', test] : []),
    ],
    { encoding: 'utf8', cwd: scratch, timeout: 30_000 },
  );
  console.log(result.stdout);
  if (result.stderr) console.error(result.stderr);
  if (result.error) throw result.error;
  return { status: result.status, output: result.stdout + result.stderr };
}

function verify(
  file: string,
  originalText: string,
  mutation: string,
  test: string,
) {
  const path = join(scratch, 'src/lua', file);
  const original = readFileSync(path, 'utf8');
  assert.equal(
    original.split(originalText).length,
    2,
    `Expected one mutation site in ${file}`,
  );
  try {
    console.log(`Mutation enabled in temporary checkout: ${file}`);
    writeFileSync(path, original.replace(originalText, mutation));
    const result = execute(test);
    assert.equal(result.status, 1, `Mutation survived in ${file}`);
    assert(
      result.output.includes('AssertionError'),
      `The test must fail on its assertion for ${file}`,
    );
  } finally {
    writeFileSync(path, original);
  }
  console.log(`Restored: ${file}`);
  assert.equal(execute(test).status, 0);
}

try {
  cpSync('src', join(scratch, 'src'), { recursive: true });
  cpSync('tests/integration', join(scratch, 'tests/integration'), {
    recursive: true,
    filter: (path) =>
      !path.endsWith('.ts') || path.endsWith('ownership-mutations.test.ts'),
  });
  cpSync('package.json', join(scratch, 'package.json'));
  assert.equal(execute().status, 0, 'Baseline failed');
  verify(
    'ack.lua',
    ' or job[4] ~= ARGV[4]',
    '',
    'fencing prevents a stale acknowledgment',
  );
  verify(
    'reap.lua',
    'if deadline > now then',
    'if false then',
    'reap refuses an active candidate',
  );
} finally {
  assert.equal(resolve(scratch, '..'), resolve(root, 'dist'));
  rmSync(scratch, { recursive: true });
}
