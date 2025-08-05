export function withAbortSignal<T>(
  promiseFn: () => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  if (!signal) return promiseFn();

  return new Promise<T>((resolve, reject) => {
    if (signal.aborted) {
      return reject(new Error("Aborted"));
    }

    let settled = false;

    const onAbort = () => {
      if (!settled) {
        settled = true;
        reject(new Error("Operation aborted"));
      }
    };

    signal.addEventListener("abort", onAbort, { once: true });

    promiseFn()
      .then((res) => {
        if (!settled) {
          settled = true;
          signal.removeEventListener("abort", onAbort);
          resolve(res);
        }
      })
      .catch((err) => {
        if (!settled) {
          settled = true;
          signal.removeEventListener("abort", onAbort);
          reject(err);
        }
      });
  });
}
