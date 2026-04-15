'use strict';

/**
 * Basic integration test.
 * Requires the evodb server to be running:
 *   evodb serve test.evodb
 */

const { EvoDB } = require('../src/client');

async function main() {
  const db = new EvoDB({ host: 'localhost', port: 7777 });

  console.log('Connecting...');
  await db.connect();
  console.log('Connected ✓');

  // Cleanup previous run
  try { await db.drop('test_users'); } catch {}

  // Forge
  await db.forge('test_users', { id: 'INT INDEX', name: 'STRING', xp: 'INT' });
  console.log('Table forged ✓');

  // Push
  await db.push('test_users', [1, 'Alice', 500]);
  await db.push('test_users', [2, 'Bob', 300]);
  console.log('Rows pushed ✓');

  // Pull all
  const rows = await db.pull('test_users');
  console.log('Pull all:', rows);
  console.assert(rows.length === 2, 'Should have 2 rows');

  // PullWhere
  const alice = await db.pullWhere('test_users', 'name', 'Alice');
  console.log('PullWhere Alice:', alice);
  console.assert(alice.length === 1);

  // Count
  const total = await db.count('test_users');
  console.log('Count:', total);
  console.assert(total === 2);

  // Reforge
  const updated = await db.reforge('test_users', { col: 'id', val: 1 }, { col: 'xp', val: 999 });
  console.log('Reforged rows:', updated);

  // Burn
  const deleted = await db.burn('test_users', 'id', 2);
  console.log('Burned rows:', deleted);
  console.assert(deleted === 1);

  // Final count
  const finalCount = await db.count('test_users');
  console.assert(finalCount === 1, `Expected 1, got ${finalCount}`);

  // Schema
  const schema = await db.schema('test_users');
  console.log('Schema:', schema);

  // Cleanup
  await db.drop('test_users');

  db.close();
  console.log('\n✓ All tests passed');
}

main().catch(err => {
  console.error('✗ Test failed:', err.message);
  process.exit(1);
});
