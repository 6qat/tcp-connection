import {
  Effect,
  TestClock,
  Fiber,
  Option,
  TestContext,
  Queue,
  Clock,
  Deferred,
} from "effect";
import { test, expect } from "bun:test";

// Bun test format
test("timeout should work correctly", async () => {
  const program = Effect.gen(function* () {
    // Create a fiber that sleeps for 5 minutes and then times out
    // after 1 minute
    const fiber = yield* Effect.sleep("5 minutes").pipe(
      Effect.timeoutTo({
        duration: "1 minute",
        onSuccess: Option.some,
        onTimeout: () => Option.none<void>(),
      }),
      Effect.fork
    );

    // Adjust the TestClock by 1 minute to simulate the passage of time
    yield* TestClock.adjust("1 minutes");

    // Get the result of the fiber
    const result = yield* Fiber.join(fiber);

    // Check if the result is None, indicating a timeout
    return Option.isNone(result);
  }).pipe(Effect.provide(TestContext.TestContext));

  // Run the Effect and check the result with Bun's expect
  const result = await Effect.runPromise(program);
  expect(result).toBeTrue();
});

test("timeout recurring effects", async () => {
  const test = Effect.gen(function* () {
    const q = yield* Queue.unbounded();

    yield* Queue.offer(q, undefined).pipe(
      // Delay the effect for 60 minutes and repeat it forever
      Effect.delay("60 minutes"),
      Effect.forever,
      Effect.fork
    );

    // Check if no effect is performed before the recurrence period
    const a = yield* Queue.poll(q).pipe(Effect.andThen(Option.isNone));

    // Adjust the TestClock by 60 minutes to simulate the passage of time
    yield* TestClock.adjust("60 minutes");

    // Check if an effect is performed after the recurrence period
    const b = yield* Queue.take(q).pipe(Effect.as(true));

    // Check if the effect is performed exactly once
    const c = yield* Queue.poll(q).pipe(Effect.andThen(Option.isNone));

    // Adjust the TestClock by another 60 minutes
    yield* TestClock.adjust("60 minutes");

    // Check if another effect is performed
    const d = yield* Queue.take(q).pipe(Effect.as(true));
    const e = yield* Queue.poll(q).pipe(Effect.andThen(Option.isNone));

    // Ensure that all conditions are met
    return a && b && c && d && e;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(test);
  expect(result).toBeTrue();
});

test("testing clock", async () => {
  const test = Effect.gen(function* () {
    // Get the current time using the Clock
    const startTime = yield* Clock.currentTimeMillis;

    // Adjust the TestClock by 1 minute to simulate the passage of time
    yield* TestClock.adjust("1 minute");

    // Get the current time again
    const endTime = yield* Clock.currentTimeMillis;

    // Check if the time difference is at least
    // 60,000 milliseconds (1 minute)
    return endTime - startTime >= 60_000;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(test);
  expect(result).toBeTrue();
});

test("testing defered", async () => {
  const test = Effect.gen(function* () {
    // Create a deferred value
    const deferred = yield* Deferred.make<number, void>();

    // Run two effects concurrently: sleep for 10 seconds and succeed
    // the deferred with a value of 1
    yield* Effect.all(
      [Effect.sleep("10 seconds"), Deferred.succeed(deferred, 1)],
      { concurrency: "unbounded" }
    ).pipe(Effect.fork);

    // Adjust the TestClock by 10 seconds
    yield* TestClock.adjust("10 seconds");

    // Await the value from the deferred
    const readRef = yield* Deferred.await(deferred);

    // Verify the deferred value is correctly set
    return readRef === 1;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(test);
  expect(result).toBeTrue();
});
