import { withAbortSignal } from "./withAbortSignal";

export type WithAbortSignalProxy<T> = T & {
  withAbortSignal(signal: AbortSignal): T;
};

export function withAbortSignalProxy<T extends object>(
  obj: T,
  defaultSignal?: AbortSignal,
): WithAbortSignalProxy<T> {
  function createProxy(signal?: AbortSignal): WithAbortSignalProxy<T> {
    const handler: ProxyHandler<T> = {
      get(target, prop, receiver) {
        if (prop === "withAbortSignal") {
          return (s: AbortSignal) => createProxy(s);
        }

        const orig = Reflect.get(target, prop, receiver);

        if (typeof orig !== "function") return orig;

        return (...args: unknown[]) =>
          withAbortSignal(() => orig.apply(receiver, args), signal);
      },
    };

    return new Proxy(obj, handler) as WithAbortSignalProxy<T>;
  }

  return createProxy(defaultSignal);
}
