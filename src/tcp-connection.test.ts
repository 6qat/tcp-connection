/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { test, expect, mock, afterAll } from 'bun:test';
import {
  Effect,
  TestClock,
  Fiber,
  Option,
  TestContext,
  Queue,
  Clock,
  Deferred,
  Duration,
} from 'effect';

import { createTcpStream, type TcpStream } from '../../m-cedro/src/tcp-stream';

// Mock Bun.Socket functionality for testing
const createMockSocket = () => {
  const writtenData: Uint8Array[] = [];

  return {
    socket: {
      write: mock((data: Uint8Array) => {
        writtenData.push(data);
        return data.length; // Simulate successful write
      }),
      end: mock(() => {
        return void 0;
      }),
    },
    writtenData,
    emitData: (handlers: any, data: Uint8Array) => {
      handlers.data?.(null, data);
    },
    emitError: (handlers: any, error: Error) => {
      handlers.error?.(null, error);
    },
    emitClose: (handlers: any) => {
      handlers.close?.(null);
    },
  };
};

// Mock Bun.connect
const originalConnect = Bun.connect;
Bun.connect = mock((options: any) => {
  const mockSocket = createMockSocket();
  const handlers = options.socket;

  mockSocket.emitData = (data) => handlers.data?.(mockSocket.socket, data);
  mockSocket.emitError = (error) => handlers.error?.(mockSocket.socket, error);
  mockSocket.emitClose = () => handlers.close?.(mockSocket.socket);

  // Use type assertion to tell TypeScript this is a Socket
  return Promise.resolve(mockSocket.socket as unknown as Bun.Socket<undefined>);
});

// Restore the original after tests
afterAll(() => {
  Bun.connect = originalConnect;
});

test('TcpStream.connect should establish a connection', async () => {
  const program = Effect.gen(function* () {
    const stream = yield* createTcpStream({
      host: 'localhost',
      port: 8080,
      timeout: Duration.seconds(1),
    });

    // Test the connection was successful
    // return stream instanceof TcpStream;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(program);
  expect(result).toBeTrue();
});

test('TcpStream.write should send data through the socket', async () => {
  const mockSocket = createMockSocket();
  let capturedHandlers: any = {};

  // Override connect just for this test with our specific mock
  Bun.connect = mock((options: any) => {
    capturedHandlers = options.socket;
    // Return a Promise and use type assertion
    return Promise.resolve(
      mockSocket.socket as unknown as Bun.Socket<undefined>,
    );
  });

  const program = Effect.gen(function* () {
    const stream = yield* createTcpStream({
      host: 'localhost',
      port: 8080,
    });

    const testData = new Uint8Array([1, 2, 3, 4]);
    const writeResult = yield* stream.send(testData);

    return {
      writeResult,
      writtenData: mockSocket.writtenData,
    };
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(program);
  expect(result.writeResult).toBeTrue();
  expect(result.writtenData.length).toBe(1);
  expect(result.writtenData[0]).toEqual(new Uint8Array([1, 2, 3, 4]));
});

test('TcpStream.writeText should properly encode and send text', async () => {
  const mockSocket = createMockSocket();
  Bun.connect = mock((options: any) =>
    Promise.resolve(mockSocket.socket as unknown as Bun.Socket<undefined>),
  );

  const program = Effect.gen(function* () {
    const stream = yield* createTcpStream({
      host: 'localhost',
      port: 8080,
    });
    const writeResult = yield* stream.sendText('Hello, world!');
    return writeResult;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(program);
  expect(result).toBeTrue();

  const encoder = new TextEncoder();
  expect(mockSocket.writtenData.length).toBe(1);
  expect(mockSocket.writtenData[0]).toEqual(encoder.encode('Hello, world!'));
});

// Bun test format
test('timeout should work correctly', async () => {
  const program = Effect.gen(function* () {
    // Create a fiber that sleeps for 5 minutes and then times out
    // after 1 minute
    const fiber = yield* Effect.sleep('5 minutes').pipe(
      Effect.timeoutTo({
        duration: '1 minute',
        onSuccess: Option.some,
        onTimeout: () => Option.none<void>(),
      }),
      Effect.fork,
    );

    // Adjust the TestClock by 1 minute to simulate the passage of time
    yield* TestClock.adjust('1 minutes');

    // Get the result of the fiber
    const result = yield* Fiber.join(fiber);

    // Check if the result is None, indicating a timeout
    return Option.isNone(result);
  }).pipe(Effect.provide(TestContext.TestContext));

  // Run the Effect and check the result with Bun's expect
  const result = await Effect.runPromise(program);
  expect(result).toBeTrue();
});

test('timeout recurring effects', async () => {
  const test = Effect.gen(function* () {
    const q = yield* Queue.unbounded();

    yield* Queue.offer(q, undefined).pipe(
      // Delay the effect for 60 minutes and repeat it forever
      Effect.delay('60 minutes'),
      Effect.forever,
      Effect.fork,
    );

    // Check if no effect is performed before the recurrence period
    const a = yield* Queue.poll(q).pipe(Effect.andThen(Option.isNone));

    // Adjust the TestClock by 60 minutes to simulate the passage of time
    yield* TestClock.adjust('60 minutes');

    // Check if an effect is performed after the recurrence period
    const b = yield* Queue.take(q).pipe(Effect.as(true));

    // Check if the effect is performed exactly once
    const c = yield* Queue.poll(q).pipe(Effect.andThen(Option.isNone));

    // Adjust the TestClock by another 60 minutes
    yield* TestClock.adjust('60 minutes');

    // Check if another effect is performed
    const d = yield* Queue.take(q).pipe(Effect.as(true));
    const e = yield* Queue.poll(q).pipe(Effect.andThen(Option.isNone));

    // Ensure that all conditions are met
    return a && b && c && d && e;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(test);
  expect(result).toBeTrue();
});

test('testing clock', async () => {
  const test = Effect.gen(function* () {
    // Get the current time using the Clock
    const startTime = yield* Clock.currentTimeMillis;

    // Adjust the TestClock by 1 minute to simulate the passage of time
    yield* TestClock.adjust('1 minute');

    // Get the current time again
    const endTime = yield* Clock.currentTimeMillis;

    // Check if the time difference is at least
    // 60,000 milliseconds (1 minute)
    return endTime - startTime >= 60_000;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(test);
  expect(result).toBeTrue();
});

test('testing defered', async () => {
  const test = Effect.gen(function* () {
    // Create a deferred value
    const deferred = yield* Deferred.make<number, void>();

    // Run two effects concurrently: sleep for 10 seconds and succeed
    // the deferred with a value of 1
    yield* Effect.all(
      [Effect.sleep('10 seconds'), Deferred.succeed(deferred, 1)],
      { concurrency: 'unbounded' },
    ).pipe(Effect.fork);

    // Adjust the TestClock by 10 seconds
    yield* TestClock.adjust('10 seconds');

    // Await the value from the deferred
    const readRef = yield* Deferred.await(deferred);

    // Verify the deferred value is correctly set
    return readRef === 1;
  }).pipe(Effect.provide(TestContext.TestContext));

  const result = await Effect.runPromise(test);
  expect(result).toBeTrue();
});
