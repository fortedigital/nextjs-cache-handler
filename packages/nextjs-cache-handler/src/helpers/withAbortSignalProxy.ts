import { withAbortSignal } from "./withAbortSignal";

export type WithAbortSignalProxy<T> = T & {
  withAbortSignal(signal: AbortSignal): T;
};

export function withAbortSignalProxy<T extends object>(
  obj: T,
  defaultSignal?: AbortSignal,
): WithAbortSignalProxy<T> {
  let signal: AbortSignal | undefined = defaultSignal;

  const handler: ProxyHandler<T> = {
    get(target, prop, receiver) {
      if (prop === "withAbortSignal") {
        return (s: AbortSignal) => {
          signal = s;
          return new Proxy(obj, handler);
        };
      }

      const orig = Reflect.get(target, prop, receiver);

      if (typeof orig !== "function") return orig;

      return (...args: unknown[]) =>
        withAbortSignal(() => (orig as Function).apply(target, args), signal);
    },
  };

  return new Proxy(obj, handler) as WithAbortSignalProxy<T>;
}
