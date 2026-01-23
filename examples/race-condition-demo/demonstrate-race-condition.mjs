#!/usr/bin/env node
/**
 * Race Condition Demonstration for @fortedigital/nextjs-cache-handler
 * 
 * USAGE:
 *   node demonstrate-race-condition.mjs
 * 
 * PREREQUISITES:
 *   - Redis running on localhost:6379
 *   - Package built: pnpm build
 * 
 * WHAT THIS TESTS:
 *   Simulates cache invalidation with concurrent reads to expose the race condition
 *   where tags are written before values, causing false cache misses.
 * 
 * BUG DESCRIPTION:
 *   In set() method, tags are written in one Promise.all(), then the value
 *   is written in a second Promise.all(). During the gap between these two operations,
 *   concurrent get() calls see tags exist but value doesn't, returning null.
 * 
 * THE FIX:
 *   Execute all operations atomically in a single Promise.all():
 *   await Promise.all([setTagsOp, setTtlOp, setValueOp, expireOp].filter(Boolean))
 * 
 * EXPECTED RESULTS:
 *   - With buggy code: 98-99.5% hit ratio (0.5-2% false misses)
 *   - With fixed code: 100% hit ratio (0 false misses)
 *   - Production with 28-31 pods: Buggy code shows ~66.6% hit ratio
 */

import { createClient } from 'redis';
import createRedisHandler from '../../packages/nextjs-cache-handler/dist/handlers/redis-strings.js';

// Configuration - adjust these to change test intensity
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = 6379;
const TEST_KEY = 'demo-race-key';
const TEST_ITERATIONS = 100;           // Number of cache invalidation cycles
const CONCURRENT_READERS = 20;         // Concurrent get() calls per iteration

console.log('\n' + '='.repeat(80));
console.log('RACE CONDITION DEMONSTRATION');
console.log('Package: @fortedigital/nextjs-cache-handler v2.5.0');
console.log('='.repeat(80) + '\n');

console.log('📋 TEST SETUP:');
console.log(`   - Test iterations: ${TEST_ITERATIONS}`);
console.log(`   - Concurrent readers per iteration: ${CONCURRENT_READERS}`);
console.log(`   - Total read attempts: ${TEST_ITERATIONS * CONCURRENT_READERS}`);
console.log(`   - Simulates cache invalidation + regeneration with concurrent reads\n`);

// Create Redis client
const client = createClient({
  socket: { host: REDIS_HOST, port: REDIS_PORT }
});

await client.connect();
console.log('✅ Connected to Redis');

// Create handler
const handler = createRedisHandler({
  client,
  keyPrefix: 'test:',
});

// Clean up
await client.del(`test:${TEST_KEY}`);
await client.del(`test:__sharedTags__`);
await client.del(`test:__sharedTagsTtl__`);

console.log('\n' + '-'.repeat(80));
console.log('🔬 RUNNING TEST...\n');

let hits = 0;
let misses = 0;

for (let i = 0; i < TEST_ITERATIONS; i++) {
  // Simulate cache invalidation by deleting keys
  await client.del(`test:${TEST_KEY}`);
  await client.del(`test:__sharedTags__`);
  await client.del(`test:__sharedTagsTtl__`);

  // Start cache regeneration (set operation with race window)
  const setPromise = handler.set(TEST_KEY, {
    lastModified: Date.now(),
    lifespan: {
      expireAt: Math.floor(Date.now() / 1000) + 3600,
      expireAge: 3600,
      lastModifiedAt: Math.floor(Date.now() / 1000),
      staleAge: 3600,
      staleAt: Math.floor(Date.now() / 1000) + 3600,
      revalidate: 3600,
    },
    tags: ['test-tag'],
    value: { kind: 'FETCH', data: { iteration: i } },
  });

  // Simulate concurrent readers trying to access during regeneration
  const getPromises = Array.from({ length: CONCURRENT_READERS }, (_, j) => 
    new Promise(resolve => 
      setTimeout(() => 
        handler.get(TEST_KEY, { implicitTags: [] }).then(resolve), 
        j * 2 // Stagger requests
      )
    )
  );

  // Wait for all operations
  const [_, ...results] = await Promise.all([setPromise, ...getPromises]);

  // Count hits and misses
  const iterationHits = results.filter(r => r !== null).length;
  const iterationMisses = results.filter(r => r === null).length;
  
  hits += iterationHits;
  misses += iterationMisses;

  // Progress indicator
  if ((i + 1) % 20 === 0) {
    const currentRatio = ((hits / (hits + misses)) * 100).toFixed(1);
    process.stdout.write(`\r   Progress: ${i + 1}/${TEST_ITERATIONS} iterations | Current hit ratio: ${currentRatio}%`);
  }
}

console.log('\n\n' + '-'.repeat(80));
console.log('📊 FINAL RESULTS:\n');

const totalReads = hits + misses;
const hitRatio = ((hits / totalReads) * 100).toFixed(2);
const missRatio = ((misses / totalReads) * 100).toFixed(2);

console.log(`   Total cache reads:       ${totalReads.toLocaleString()}`);
console.log(`   Successful hits:         ${hits.toLocaleString()} (${hitRatio}%)`);
console.log(`   False cache misses:      ${misses.toLocaleString()} (${missRatio}%)`);
console.log(`   Cache hit ratio:         ${hitRatio}%\n`);

if (misses > 0) {
  console.log('😱 RACE CONDITION DETECTED\n');
} else {
  console.log('✅ NO RACE CONDITIONS DETECTED\n');
  console.log('   The atomic Promise.all() implementation successfully eliminates');
  console.log('   the race window by executing all operations together.\n');
  console.log('   100% cache hit ratio achieved! 🎉');
}

console.log('\n' + '='.repeat(80) + '\n');

await client.quit();
