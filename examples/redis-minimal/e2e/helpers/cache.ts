import { expect, type APIRequestContext, type Page } from '@playwright/test';

export const BUILD_TIMESTAMP_TEST_ID = 'build-timestamp';

export async function getBuildTimestamp(page: Page) {
  return page.getByTestId(BUILD_TIMESTAMP_TEST_ID).first().textContent();
}

export async function waitForBuildTimestamp(page: Page) {
  await expect(page.getByTestId(BUILD_TIMESTAMP_TEST_ID).first()).not.toBeEmpty();
}

export async function revalidatePath(
  request: APIRequestContext,
  path: string,
  baseURL = process.env.BASE_URL ?? 'http://127.0.0.1:3000',
) {
  const response = await request.post(`${baseURL}/api/revalidate`, {
    data: { path },
  });
  expect(response.ok()).toBeTruthy();
}

export async function revalidateTag(
  request: APIRequestContext,
  tag: string,
  cacheLife: 'max' | 'hours' | 'days' = 'max',
  baseURL = process.env.BASE_URL ?? 'http://127.0.0.1:3000',
) {
  const response = await request.post(`${baseURL}/api/revalidate`, {
    data: { tag, cacheLife },
  });
  expect(response.ok()).toBeTruthy();
}
