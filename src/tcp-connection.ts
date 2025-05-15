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
} from 'effect';
import { NodeRuntime } from '@effect/platform-node';

// Base error class with a more flexible tag system
export class TcpConnectionError extends Data.TaggedError('TcpConnectionError') {
  constructor(readonly errorType?: string) {
    super();
  }
}

// Specific error type that extends the base error
export class BunError extends TcpConnectionError {
  constructor(readonly message: string) {
    super('BunError');
  }
}

export class TcpConnectionTimeoutError extends TcpConnectionError {
  constructor(readonly message: string) {
    super('ConnectionTimeout');
  }
}

export class TcpConnectionWriteError extends TcpConnectionError {
  constructor(
    readonly message: string,
    readonly bytesWritten: number,
    readonly totalBytes: number,
  ) {
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
  readonly send: (data: Uint8Array) => Effect.Effect<void, Error>;
}

// Configuration (host/port)
class TcpConfig extends Context.Tag('TcpConfig')<
  TcpConfig,
  { host: string; port: number }
>() {}

// tcp.ts (continued)
const TcpConnectionLive = Layer.scoped(
  TcpConnection,
  Effect.gen(function* () {
    const { host, port } = yield* TcpConfig;
    const queue = yield* Queue.unbounded<Uint8Array>();
    const net = yield* Effect.promise(() => import('node:net'));

    // Create TCP socket
    const socket = net.createConnection({ host, port });

    // Handle incoming data
    socket.on('data', (chunk: Buffer) => {
      Effect.runPromise(Queue.offer(queue, new Uint8Array(chunk)));
    });

    // Cleanup on exit
    yield* Effect.addFinalizer(() => Effect.sync(() => socket.end()));

    return {
      incoming: Stream.fromQueue(queue),
      send: (data: Uint8Array) =>
        Effect.tryPromise({
          try: () =>
            new Promise((resolve, reject) => {
              socket.write(data, (err) => (err ? reject(err) : resolve()));
            }),
          catch: (error) => new Error(`Send failed: ${error}`),
        }),
    };
  }),
);

// Usage examples
const program = Effect.gen(function* () {
  const client = yield* TcpConnection;

  // yield* client.send('GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n');
  const data = new TextEncoder().encode(
    'GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n',
  );
  yield* client.send(data);

  yield* pipe(
    client.incoming,
    Stream.takeUntil((data) => data.includes(0x04)), // EOT (End of transmission)
    Stream.tap((data) => Effect.logDebug(new TextDecoder().decode(data))),
    Stream.tap((chunks) => Effect.logDebug(`Received ${chunks.length} chunks`)),
    Stream.runDrain,
  );
}).pipe(Effect.catchAll((error) => Effect.logError(error)));

const LoggerLive = Logger.minimumLogLevel(LogLevel.Debug);
const TcpConfigLive = Layer.succeed(TcpConfig, {
  host: 'www.terra.com.br',
  port: 80,
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
