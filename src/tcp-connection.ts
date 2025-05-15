// src/tcp-stream.ts
import {
  Effect,
  Deferred,
  Stream,
  Queue,
  Fiber,
  pipe,
  Chunk,
  Duration,
  Schedule,
  Ref,
  identity,
  Data,
  Context,
  Layer,
  LogLevel,
  Logger,
  Console,
  type Scope,
  Metric,
} from 'effect';
import { NodeRuntime } from '@effect/platform-node';
import type { Socket } from 'node:net';

// Base error class with a more flexible tag system
export class TcpConnectionError extends Data.TaggedError('TcpConnectionError') {
  constructor(readonly errorType?: string) {
    super();
  }
}

export class TcpConnectionTimeoutError extends TcpConnectionError {
  constructor(readonly message: string) {
    super('ConnectionTimeout');
  }
}

export class TcpConnectionWriteError extends TcpConnectionError {
  constructor(readonly message: string) {
    super('WriteError');
  }
}

export class TcpConnectionCloseError extends TcpConnectionError {
  constructor(readonly message: string) {
    super('CloseError');
  }
}

// Context Tag for Dependency Injection
class TcpConnection extends Context.Tag('TcpConnection')<
  TcpConnection,
  TcpConnectionShape
>() {}

interface TcpConnectionShape {
  readonly incoming: Stream.Stream<Uint8Array>;
  readonly send: (data: Uint8Array) => Effect.Effect<void, TcpConnectionError>;
  readonly sendWithRetry: (
    data: Uint8Array,
  ) => Effect.Effect<void, TcpConnectionError>;
}

// Configuration (host/port)
class TcpConfig extends Context.Tag('TcpConfig')<
  TcpConfig,
  { host: string; port: number; bufferSize?: number }
>() {}

// tcp.ts (continued)
const TcpConnectionLive = Layer.scoped(
  TcpConnection,
  Effect.gen(function* () {
    const { host, port, bufferSize = 2048 } = yield* TcpConfig;
    const queue = yield* Queue.bounded<Uint8Array>(bufferSize);
    const net = yield* Effect.promise(() => import('node:net'));

    // Reconnect on failure
    const createSocket: Effect.Effect<Socket, TcpConnectionError, never> =
      Effect.async((resume) => {
        const socket = net.createConnection({ host, port });
        socket.on('connect', () => resume(Effect.succeed(socket)));
        socket.on('error', (err) =>
          resume(Effect.fail(new TcpConnectionError(err.message))),
        );
      });

    // Create TCP socket
    const socket = yield* createSocket.pipe(
      Effect.retry(
        Schedule.exponential('1 second').pipe(
          Schedule.compose(Schedule.recurUpTo(5)),
        ),
      ),
    );

    // Handle incoming data
    socket.on('data', (chunk: Buffer) => {
      Effect.runPromise(Queue.offer(queue, new Uint8Array(chunk)));
    });

    socket.on('close', () => {
      Effect.runPromise(Effect.logDebug('Socket closed'));
    });

    // Add error handling to socket setup
    socket.on('error', (err) => {
      Effect.runPromise(Effect.fail(new TcpConnectionError(err.message)));
    });

    const send = (data: Uint8Array) =>
      Effect.tryPromise({
        try: () =>
          new Promise((resolve, reject) => {
            socket.write(data, (err) => (err ? reject(err) : resolve(void 0)));
          }),
        catch: (error) => {
          return new TcpConnectionWriteError(`Send failed: ${error}`);
        },
      });

    // Retry logic for send
    const sendWithRetry = (data: Uint8Array) =>
      send(data).pipe(
        Effect.retry(
          Schedule.exponential('100 millis', 2).pipe(
            Schedule.compose(Schedule.recurUpTo(5)),
          ),
        ),
        Effect.catchAll((error) => new TcpConnectionWriteError(error.message)),
      );

    // Cleanup on exit
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Effect.sync(() => socket.end());
        yield* Queue.shutdown(queue);
      }),
    );

    return {
      incoming: Stream.fromQueue(queue),
      send,
      sendWithRetry,
    };
  }),
);

// Usage examples

// Metrics
const bytesReceived = Metric.counter('tcp.bytes_received');
const bytesSent = Metric.counter('tcp.bytes_sent');

const program = Effect.gen(function* () {
  // Create a Ref to track the value
  const bytesReceivedRef = yield* Ref.make(0);
  const bytesSentRef = yield* Ref.make(0);

  const printMetrics = Effect.gen(function* () {
    const received = yield* Ref.get(bytesReceivedRef);
    const sent = yield* Ref.get(bytesSentRef);
    yield* Effect.logInfo(`Bytes received: ${received}`);
    yield* Effect.logInfo(`Bytes sent: ${sent}`);
  });

  const client = yield* TcpConnection;

  // yield* client.send('GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n');
  const data = new TextEncoder().encode(
    'GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n',
  );

  const send = (data: Uint8Array) =>
    pipe(
      Ref.update(bytesSentRef, (n) => n + data.length),
      Effect.flatMap(() => client.send(data)),
    );
  const sendWithRetry = (data: Uint8Array) =>
    pipe(
      Ref.update(bytesSentRef, (n) => n + data.length),
      Effect.flatMap(() => client.sendWithRetry(data)),
    );

  yield* send(data).pipe(Effect.orElse(() => Effect.logError('deu ruim')));

  yield* pipe(
    client.incoming,
    Stream.tap((data) => Metric.incrementBy(bytesReceived, data.length)),
    Stream.tap((data) => Ref.update(bytesReceivedRef, (n) => n + data.length)),
    // Stream.tap((data) => Effect.logDebug(new TextDecoder().decode(data))),
    Stream.tap((data) => Effect.logDebug(`Received: ${Buffer.from(data)}`)),
    Stream.tap((chunks) => Effect.logDebug(`Received ${chunks.length} chunks`)),
    Stream.runDrain,
    Effect.fork, // Run in background
  );

  // Send "ping" every second
  yield* pipe(
    Effect.repeat(
      pipe(
        Effect.flatMap(
          Effect.sync(() => Buffer.from('ping')),
          (data) =>
            sendWithRetry(data).pipe(
              Effect.orElse(() => Effect.logError('deu ruim')),
              Effect.zipRight(printMetrics), // <-- Add this line
            ),
        ),
      ),
      Schedule.spaced('1 seconds'),
    ),
    Effect.fork,
  );

  yield* Effect.never;
}).pipe(Effect.catchAll((error) => Effect.logError(error)));

const LoggerLive = Logger.minimumLogLevel(LogLevel.Debug);
const TcpConfigLive = Layer.succeed(TcpConfig, {
  host: 'www.terra.com.br',
  port: 80,
  bufferSize: 1024,
});

const runnable = program.pipe(
  Effect.provide(
    TcpConnectionLive.pipe(
      Layer.provideMerge(TcpConfigLive.pipe(Layer.provideMerge(LoggerLive))),
    ),
  ),
);

NodeRuntime.runMain(runnable);

export { TcpConnection, TcpConnectionLive };
