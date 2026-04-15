#!/usr/bin/env node
'use strict';

/**
 * Compiles the evodb server binary.
 * Run: node scripts/build.js [linux|darwin|windows]
 *
 * Output:
 *   bin/evodb          (linux/mac)
 *   bin/evodb.exe      (windows)
 */

const { execSync, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');

const srcDir = path.join(__dirname, '..', 'server-src');
const binDir = path.join(__dirname, '..', 'bin');

const target = process.argv[2] || process.platform;
const platformMap = {
  linux:   { GOOS: 'linux',   GOARCH: 'amd64', ext: ''     },
  darwin:  { GOOS: 'darwin',  GOARCH: 'amd64', ext: ''     },
  win32:   { GOOS: 'windows', GOARCH: 'amd64', ext: '.exe' },
  windows: { GOOS: 'windows', GOARCH: 'amd64', ext: '.exe' },
};

const plat = platformMap[target] || platformMap.linux;
const outFile = path.join(binDir, `evodb${plat.ext}`);

fs.mkdirSync(binDir, { recursive: true });

console.log(`Building evodb for ${plat.GOOS}/${plat.GOARCH}...`);

const result = spawnSync('go', ['build', '-o', outFile, '.'], {
  cwd: srcDir,
  env: { ...process.env, GOOS: plat.GOOS, GOARCH: plat.GOARCH, CGO_ENABLED: '0' },
  stdio: 'inherit',
});

if (result.status !== 0) {
  console.error('Build failed. Make sure Go is installed: https://go.dev/dl/');
  process.exit(1);
}

fs.chmodSync(outFile, 0o755);
console.log(`\n✓ Binary ready: ${outFile}`);
console.log(`  Usage: evodb serve mydb.evodb`);
console.log(`         evodb repl`);
