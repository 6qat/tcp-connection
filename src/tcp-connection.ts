import type { Socket } from 'node:net';
import { NodeRuntime } from '@effect/platform-node';
// src/tcp-stream.ts
import {
  Context,
  Data,
  Effect,
  Fiber,
  Layer,
  LogLevel,
  Logger,
  Metric,
  Queue,
  Ref,
  Runtime,
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
  {
    host: string;
    port: number;
    bufferSize?: number;
    runtimeEffect?: Effect.Effect<Runtime.Runtime<never>, never, never>;
  }
>() {}

// State
interface ConnectionState {
  readonly socket: Socket; // Or TLS socket
  readonly isOpen: boolean;
}

const TcpConnectionLive = Layer.scoped(
  TcpConnection,
  Effect.gen(function* () {
    const {
      host,
      port,
      bufferSize = 2048,
      runtimeEffect = Effect.succeed(Runtime.defaultRuntime),
    } = yield* TcpConfig;
    const queue = yield* Queue.bounded<Uint8Array>(bufferSize);
    const stateRef = yield* Ref.make<ConnectionState | null>(null);
    const restartQueue = yield* Queue.unbounded<void>(); // Signal restarts

    const runtime = yield* runtimeEffect;
    const runPromise = <A, E>(effect: Effect.Effect<A, E, never>) =>
      Runtime.runPromise(runtime)(effect);

    const net = yield* Effect.promise(() => import('node:net'));

    // When the socket closes, notify the restart queue
    const handleClose = Effect.gen(function* () {
      yield* Ref.set(stateRef, null);
      yield* Queue.offer(restartQueue, void 0); // Signal restart
      yield* Effect.logDebug('Connection closed abruptly!!!');
    });

    const handleShutdown = Effect.gen(function* () {
      yield* handleClose;
      yield* Effect.logInfo('Finalizer: shutting down queues...');
      yield* Queue.shutdown(queue);
      yield* Queue.shutdown(restartQueue);
      yield* Effect.logInfo('Finalizer: shutting down socket...');
      const currentState = yield* Ref.get(stateRef);
      currentState?.socket.end(() => console.log('Fechando o NODE'));
      currentState?.socket.destroy();
      currentState?.socket.resetAndDestroy();
      currentState?.socket.unref();
      currentState?.socket.destroySoon();
      yield* Effect.logInfo('Finalizer: done.');
    });

    const createSocket: () => Effect.Effect<
      ConnectionState,
      TcpConnectionError,
      never
    > = () => {
      const effect: Effect.Effect<ConnectionState, TcpConnectionError, never> =
        Effect.async((resume) => {
          const socket = net.createConnection({ host, port });
          socket.on('connect', () => {
            // Handle incoming data
            socket.on('data', (chunk: Buffer) => {
              Effect.runPromise(Queue.offer(queue, new Uint8Array(chunk)));
            });
            socket.on('error', (err) => {
              Effect.runPromise(
                Effect.fail(new TcpConnectionError(err.message)),
              );
            });
            socket.on('close', () => {
              runPromise(handleClose);
            });
            resume(Effect.succeed({ socket, isOpen: true }));
          });
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
        yield* handleShutdown;
      }),
    );

    return {
      incoming: Stream.fromQueue(queue, { shutdown: true }),
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
    // Effect.forever,
    Effect.fork, // Run in background
  );

  // Send "ping" every second
  const _ping = pipe(
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
      Schedule.spaced('2 seconds'),
    ),
    Effect.fork,
  );
  yield* _ping;

  // Cleanup on exit
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      yield* Queue.shutdown(client.restartQueue);
      yield* Effect.logDebug('Program Finalizer');
      yield* Effect.logDebug('Before printing stats');
      yield* printMetrics;
    }),
  );

  // Restart on close events (no polling!)
  yield* pipe(
    Stream.fromQueue(client.restartQueue, { shutdown: true }),
    Stream.tap(() => Effect.logDebug('Queue restarted')),
    Stream.tap(() => client.restart),
    Stream.runDrain,
    Effect.onInterrupt(() =>
      Effect.logDebug('Queue restart fiber interrupted'),
    ),
    // Effect.fork,
  );
}).pipe(Effect.catchAll((error) => Effect.logError(error)));

const LoggerLive = Logger.minimumLogLevel(LogLevel.Debug);
const TcpConfigLive = Layer.succeed(TcpConfig, {
  host: 'www.terra.com.br',
  port: 80,
  bufferSize: 1024,
  runtimeEffect: Effect.scoped(Layer.toRuntime(LoggerLive)),
});

const runnable = Effect.gen(function* () {
  yield* Effect.scoped(
    Effect.provide(
      Effect.provide(Effect.provide(program, TcpConnectionLive), TcpConfigLive),
      LoggerLive,
    ),
  );
});

NodeRuntime.runMain(runnable);

export { TcpConnection, TcpConnectionLive };
