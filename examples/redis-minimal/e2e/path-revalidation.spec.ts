import { expect, test } from '@playwright/test';
import {
  getBuildTimestamp,
  revalidatePath,
  waitForBuildTimestamp,
} from './helpers/cache';

test.describe('App Router', () => {
  test('/examples/default-cache', async ({ page, request }) => {
    const path = '/examples/default-cache';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/no-store', async ({ page, request }) => {
    const path = '/examples/no-store';
    let first: string | null;
    let second: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload produces a new timestamp (no-store)', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      second = await getBuildTimestamp(page);
      expect(second).not.toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(second);
    });
  });

  test('/examples/time-based-revalidation', async ({ page, request }) => {
    const path = '/examples/time-based-revalidation';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Immediate consecutive reloads keep timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);

      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/fetch-tags', async ({ page, request }) => {
    const path = '/examples/fetch-tags';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/unstable-cache', async ({ page, request }) => {
    const path = '/examples/unstable-cache';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/revalidate-tag-cachelife', async ({ page, request }) => {
    const path = '/examples/revalidate-tag-cachelife';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/update-tag', async ({ page, request }) => {
    const path = '/examples/update-tag';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/isr/blog/1', async ({ page, request }) => {
    const path = '/examples/isr/blog/1';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Immediate reload keeps timestamp (on-demand ISR)', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/static-params/cache', async ({ page, request }) => {
    const path = '/examples/static-params/cache';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/static-params/test1', async ({ page, request }) => {
    const path = '/examples/static-params/test1';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Immediate reload keeps timestamp (on-demand)', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/static-params/test2', async ({ page, request }) => {
    const path = '/examples/static-params/test2';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Immediate reload keeps timestamp (on-demand)', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });
});

test.describe('Pages Router', () => {
  test('/examples/pages-router-navigation', async ({ page, request }) => {
    const path = '/examples/pages-router-navigation';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/pages-router-navigation/1', async ({ page, request }) => {
    const path = '/examples/pages-router-navigation/1';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/pages-router-navigation/2', async ({ page, request }) => {
    const path = '/examples/pages-router-navigation/2';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });

  test('/examples/pages-router-navigation/3', async ({ page, request }) => {
    const path = '/examples/pages-router-navigation/3';
    let first: string | null;

    await test.step('Load page and capture build timestamp', async () => {
      await page.goto(path);
      await waitForBuildTimestamp(page);
      first = await getBuildTimestamp(page);
    });

    await test.step('Reload without revalidation keeps timestamp', async () => {
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).toBe(first);
    });

    await test.step('revalidatePath updates timestamp on reload', async () => {
      await revalidatePath(request, path);
      await page.reload();
      await waitForBuildTimestamp(page);
      expect(await getBuildTimestamp(page)).not.toBe(first);
    });
  });
});
