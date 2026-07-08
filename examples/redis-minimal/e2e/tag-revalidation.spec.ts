import { expect, test } from '@playwright/test';
import {
  getBuildTimestamp,
  revalidateTag,
  waitForBuildTimestamp,
} from './helpers/cache';

test('/examples/fetch-tags revalidates via tag "futurama"', async ({
  page,
  request,
}) => {
  const path = '/examples/fetch-tags';
  const tag = 'futurama';
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

  await test.step('revalidateTag updates timestamp on reload', async () => {
    await revalidateTag(request, tag);
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});

test('/examples/update-tag revalidates via tag "posts"', async ({
  page,
  request,
}) => {
  const path = '/examples/update-tag';
  const tag = 'posts';
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

  await test.step('revalidateTag updates timestamp on reload', async () => {
    await revalidateTag(request, tag);
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});

test('/examples/update-tag revalidates via tag "user-profile"', async ({
  page,
  request,
}) => {
  const path = '/examples/update-tag';
  const tag = 'user-profile';
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

  await test.step('revalidateTag updates timestamp on reload', async () => {
    await revalidateTag(request, tag);
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});

test('/examples/revalidate-tag-cachelife revalidates via tag "cachelife-max"', async ({
  page,
  request,
}) => {
  const path = '/examples/revalidate-tag-cachelife';
  const tag = 'cachelife-max';
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

  await test.step('revalidateTag updates timestamp on reload', async () => {
    await revalidateTag(request, tag);
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});

test('/examples/revalidate-tag-cachelife revalidates via tag "cachelife-hours"', async ({
  page,
  request,
}) => {
  const path = '/examples/revalidate-tag-cachelife';
  const tag = 'cachelife-hours';
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

  await test.step('revalidateTag updates timestamp on reload', async () => {
    await revalidateTag(request, tag);
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});

test('/examples/revalidate-tag-cachelife revalidates via tag "cachelife-days"', async ({
  page,
  request,
}) => {
  const path = '/examples/revalidate-tag-cachelife';
  const tag = 'cachelife-days';
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

  await test.step('revalidateTag updates timestamp on reload', async () => {
    await revalidateTag(request, tag);
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});
