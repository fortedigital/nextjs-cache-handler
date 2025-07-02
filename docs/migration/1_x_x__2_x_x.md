# Migration

## 1.x.x → 2.x.x

> **Note:** Starting in `2.0.0`, `@neshca/cache-handler` is no longer a dependency.

**Requirements:**

- Next.js `>=15.2.4`
- Redis `>=5.5.6`

If you're only using the Redis client:

```json
"next": ">=15.2.4",
"@redis/client": ">=5.5.6"
```

If you need the full Redis package:

```json
"next": ">=15.2.4",
"redis": ">=5.5.6"
```

## Code changes

**Before (1.x.x):**

```js
const {
  Next15CacheHandler,
} = require("@fortedigital/nextjs-cache-handler/next-15-cache-handler");
module.exports = new Next15CacheHandler();
```

**After (2.x.x+):**

```js
const { CacheHandler } = require("@fortedigital/nextjs-cache-handler");
module.exports = CacheHandler;
```

`createBufferStringHandler` is integrated into `redis-strings` and no longer required separately for Next 15+.

---
