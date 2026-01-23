# Race Condition Testing Guide

This example demonstrates and tests the race condition bug fix in the Redis cache handler.

## Prerequisites

1. **Redis server** running locally:
   ```bash
   # macOS with Homebrew
   brew install redis
   brew services start redis
   
   # Or using Docker
   docker run -d -p 6379:6379 redis:latest
   ```

2. **Dependencies installed** (from repository root):
   ```bash
   cd ../..
   pnpm install
   ```

3. **Package built** (from repository root):
   ```bash
   cd ../..
   pnpm build
   ```

## Running the Test

From this directory:

```bash
node demonstrate-race-condition.mjs
```

Or from the repository root:

```bash
node examples/race-condition-demo/demonstrate-race-condition.mjs
```

This runs 100 iterations with 20 concurrent readers each (2,000 total reads), demonstrating:
- **With buggy code**: Race conditions occur (some % of false cache misses)
- **With fixed code**: 100% cache hit ratio

**Expected output:**
```
================================================================================
RACE CONDITION DEMONSTRATION
Package: @fortedigital/nextjs-cache-handler v2.5.0
================================================================================

📋 TEST SETUP:
   - Test iterations: 100
   - Concurrent readers per iteration: 20
   - Total read attempts: 2,000
   - Simulates cache invalidation + regeneration with concurrent reads

✅ Connected to Redis

--------------------------------------------------------------------------------
🔬 RUNNING TEST...

   Progress: 100/100 iterations | Current hit ratio: 100.0%

--------------------------------------------------------------------------------
📊 FINAL RESULTS:

   Total cache reads:       2,000
   Successful hits:         2,000 (100.00%)
   False cache misses:      0 (0.00%)
   Cache hit ratio:         100.00%

✅ NO RACE CONDITIONS DETECTED

   The atomic Promise.all() implementation successfully eliminates
   the race window by executing all operations together.

   100% cache hit ratio achieved! 🎉

================================================================================
```

### Customizing the Test

Edit `demonstrate-race-condition.mjs` to adjust:

```javascript
const TEST_ITERATIONS = 100;        // Number of cache invalidation cycles
const CONCURRENT_READERS = 20;      // Concurrent get() calls per iteration
```

More iterations and readers = more thorough testing but longer runtime.

## Understanding the Results

### Localhost vs Production

**Localhost (test environment):**
- Redis on same machine = very fast (<1ms round-trip)
- Race window is tiny = low miss rate (~0.5-2%)
- Still proves the bug exists

**Production (28-31 Kubernetes pods):**
- Network latency to Redis (~5-10ms)
- High concurrency from many pods
- Race window is much larger = high miss rate (~33%)
- This manifests as the observed 66.6% cache hit ratio

### Why the fix works

The bug occurs because Redis operations are split:

```typescript
// BUGGY: Two separate Promise.all() calls
await Promise.all([setTagsOp, setTtlOp]);     // Step 1
// ⚠️ RACE WINDOW HERE - concurrent get() might see tags but no value
await Promise.all([setValueOp, expireOp]);    // Step 2
```

The fix executes all operations atomically:

```typescript
// FIXED: Single atomic Promise.all()
await Promise.all([setTagsOp, setTtlOp, setValueOp, expireOp]);
```

No gap = no race condition.

## What the Test Does

1. **Cleans Redis** - Removes any existing test keys
2. **Runs iterations** - Each iteration:
   - Deletes cache keys (simulates invalidation)
   - Starts a `set()` operation (regenerating cache)
   - Fires 20 concurrent `get()` operations
   - Counts how many `get()` calls return `null` vs actual data
3. **Reports results** - Shows hit ratio and race condition detection

## Expected Behavior

| Code State | Local Hit Ratio | Production Hit Ratio |
|------------|----------------|---------------------|
| Buggy      | 98-99.5%       | ~66.6%             |
| Fixed      | 100%           | ~100%              |

The lower local miss rate is due to Redis being on localhost (faster, smaller race window).
