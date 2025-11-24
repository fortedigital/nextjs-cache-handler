"use server";

import { updateTag } from "next/cache";

export async function createPostWithUpdateTag(formData: FormData) {
  formData.get("title");
  formData.get("content");

  updateTag("posts");

  return {
    message: `Post created! Cache for tag 'posts' immediately expired. Refresh to see fresh data.`,
  };
}

export async function updateUserSettings(formData: FormData) {
  formData.get("theme");
  formData.get("notifications");

  updateTag("user-profile");

  return {
    message: `Settings updated! Cache immediately expired. Your changes are now visible.`,
  };
}
