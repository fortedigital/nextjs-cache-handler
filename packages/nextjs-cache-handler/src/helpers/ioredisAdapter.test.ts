import type { RedisClientType } from "@redis/client";
import type { Redis } from "ioredis";
import { ioredisAdapter } from "./ioredisAdapter";

describe("ioredisAdapter", () => {
  it("translates node-redis eval options to ioredis arguments", async () => {
    const evalCommand = jest.fn().mockResolvedValue(1);
    const client = { eval: evalCommand } as unknown as Redis;
    const adapted = ioredisAdapter(client) as RedisClientType;

    await adapted.eval("return 1", {
      keys: ["metadata"],
      arguments: ["field", "value", "60"],
    });

    expect(evalCommand).toHaveBeenCalledWith(
      "return 1",
      1,
      "metadata",
      "field",
      "value",
      "60",
    );
  });
});
