import type { Socket } from 'node:net';
import { NodeRuntime } from '@effect/platform-node';
// src/tcp-stream.ts
import {
  Context,
  Data,
  Effect,
  Layer,
  LogLevel,
  Logger,
  Metric,
  Queue,
  Ref,
  Schedule,
  Scope,
  Stream,
  pipe,
} from 'effect';

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
  readonly incoming: Stream.Stream<Uint8Array, TcpConnectionError>;
  readonly send: (data: Uint8Array) => Effect.Effect<void, TcpConnectionError>;
  readonly sendWithRetry: (
    data: Uint8Array,
  ) => Effect.Effect<void, TcpConnectionError>;
  readonly restart: Effect.Effect<void, TcpConnectionError>;
  readonly restartQueue: Queue.Queue<void>;
}

// Configuration (host/port)
class TcpConfig extends Context.Tag('TcpConfig')<
  TcpConfig,
  { host: string; port: number; bufferSize?: number }
>() {}

// State
interface ConnectionState {
  readonly socket: Socket; // Or TLS socket
  readonly isOpen: boolean;
}

const TcpConnectionLive = Layer.scoped(
  TcpConnection,
  Effect.gen(function* () {
    const { host, port, bufferSize = 2048 } = yield* TcpConfig;
    const queue = yield* Queue.bounded<Uint8Array>(bufferSize);
    const stateRef = yield* Ref.make<ConnectionState | null>(null);
    const restartQueue = yield* Queue.unbounded<void>(); // Signal restarts

    const net = yield* Effect.promise(() => import('node:net'));

    // When the socket closes, notify the restart queue
    const handleClose = () =>
      Effect.runPromise(
        Effect.gen(function* () {
          yield* Ref.set(stateRef, null);
          yield* Queue.shutdown(queue);
          yield* Queue.offer(restartQueue, void 0); // Signal restart
          yield* Effect.logWarning('Connection closed abruptly');
        }),
      );

    const createSocket: () => Effect.Effect<
      ConnectionState,
      TcpConnectionError,
      never
    > = () => {
      const effect: Effect.Effect<ConnectionState, TcpConnectionError, never> =
        Effect.async((resume) => {
          const socket = net.createConnection({ host, port });
          socket.on('connect', () =>
            resume(Effect.succeed({ socket, isOpen: true })),
          );
          socket.on('error', (err) =>
            resume(Effect.fail(new TcpConnectionError(err.message))),
          );
        });
      return effect.pipe(
        Effect.retry(
          Schedule.exponential('1 second').pipe(
            Schedule.compose(Schedule.recurUpTo(5)),
          ),
        ),
        Effect.tap(() => Effect.logInfo('Connection established')),

        Effect.tapErrorCause((cause) =>
          Effect.logError('Connection failed', cause),
        ),
      );
    };

    // Create TCP socket
    const initialState = yield* createSocket();

    yield* Ref.set(stateRef, initialState);

    // Handle incoming data
    initialState.socket.on('data', (chunk: Buffer) => {
      Effect.runPromise(Queue.offer(queue, new Uint8Array(chunk)));
    });

    // Add error handling to socket setup
    initialState.socket.on('error', (err) => {
      Effect.runPromise(Effect.fail(new TcpConnectionError(err.message)));
    });

    // Handle abrupt closure
    initialState.socket.on('close', () => {
      handleClose();
    });

    // Restart logic
    const restart: Effect.Effect<void, TcpConnectionError, never> = Effect.gen(
      function* () {
        yield* Effect.logInfo('Restarting connection');
        const currentState = yield* Ref.get(stateRef);
        if (currentState?.isOpen) return; // Already connected
        yield* createSocket().pipe(
          Effect.flatMap((state) => Ref.set(stateRef, state)),
          Effect.retry(Schedule.fibonacci('5 seconds')),
        );
      },
    );

    const send = (data: Uint8Array) =>
      Effect.gen(function* () {
        const state = yield* Ref.get(stateRef);
        if (!state?.isOpen) {
          return yield* Effect.fail(new TcpConnectionCloseError('not_open'));
        }
        return yield* Effect.tryPromise({
          try: () =>
            new Promise((resolve, reject) => {
              state.socket.write(data, (err) =>
                err
                  ? reject(new TcpConnectionWriteError(err.message))
                  : resolve(void 0),
              );
            }),
          catch: (error) => new TcpConnectionWriteError(error as string),
        });
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
        yield* Effect.sync(() => initialState.socket.end());
        yield* Queue.shutdown(queue);
      }),
    );

    return {
      incoming: Stream.fromQueue(queue).pipe(
        Stream.catchAll(() =>
          Stream.fail(new TcpConnectionError('Connection closed')),
        ),
      ),
      send,
      sendWithRetry,
      restart,
      restartQueue,
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

  // Restart on close events (no polling!)
  yield* pipe(
    Stream.fromQueue(client.restartQueue),
    Stream.tap(() => client.restart),
    Stream.runDrain,
    Effect.fork,
  );
  // yield* client.send('GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n');
  const data = new TextEncoder().encode(
    'GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n',
  );

  const send = (data: Uint8Array) =>
    pipe(
      Ref.update(bytesSentRef, (n) => n + data.length),
      Effect.zipRight(Metric.incrementBy(bytesSent, data.length)),
      Effect.flatMap(() => client.send(data)),
    );
  const sendWithRetry = (data: Uint8Array) =>
    pipe(
      Ref.update(bytesSentRef, (n) => n + data.length),
      Effect.zipRight(Metric.incrementBy(bytesSent, data.length)),
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
    Stream.catchAll((error) => {
      if (error instanceof TcpConnectionCloseError) {
        return Effect.log('Connection closed. Restarting...');
      }
      return Effect.fail(error);
    }),
    Stream.runDrain,
    Effect.forever,
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
              Effect.catchAll((error) => Effect.logError(error.message)),
              // Effect.zipRight(printMetrics), // <-- Add this line
            ),
        ),
      ),
      Schedule.spaced('3 seconds'),
    ),
    Effect.fork,
  );

  const shutdownSignal = Effect.async((resume) => {
    const onExit = () => resume(Effect.void);
    // Remove NodeRuntime default listeners
    process.removeAllListeners('SIGINT');
    process.removeAllListeners('SIGTERM');
    process.once('SIGINT', onExit);
    process.once('SIGTERM', onExit);
  });

  yield* shutdownSignal;
  yield* printMetrics;
}).pipe(Effect.catchAll((error) => Effect.logError(error)));

const LoggerLive = Logger.minimumLogLevel(LogLevel.Debug);
const TcpConfigLive = Layer.succeed(TcpConfig, {
  host: 'www.terra.com.br',
  port: 80,
  bufferSize: 1024,
});

const runnable = Effect.gen(function* () {
  const scope = yield* Scope.make();
  yield* program.pipe(
    Effect.provideService(Scope.Scope, scope),
    Effect.provide(
      TcpConnectionLive.pipe(
        Layer.provideMerge(TcpConfigLive.pipe(Layer.provideMerge(LoggerLive))),
      ),
    ),
  );
});

NodeRuntime.runMain(runnable);

export { TcpConnection, TcpConnectionLive };
