import { NodeRuntime } from '@effect/platform-node';
import type { Socket } from 'node:net';
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
  Runtime,
  Schedule,
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

    /**
     * The close event triggers runPromise(handleClose), which sets stateRef to null and signals a restart. If a new connection is established before the previous one is fully cleaned up, you could have overlapping sockets or fibers.
     */
    // TODO: Improvement: Ensure that any socket cleanup is fully completed before a new socket is created. Consider using a mutex or a dedicated "connection management" fiber to serialize open/close/restart operations.
    // When the socket closes, notify the restart queue
    const handleClose = Effect.gen(function* () {
      const currentState = yield* Ref.get(stateRef);
      const socket = currentState?.socket;
      if (socket && !socket.closed) {
        // TODO: Check if its necessary to check if the socket is closed before end()
        socket.removeAllListeners().end();
        if (!socket.destroyed) {
          socket.destroy();
        }
      }
      yield* Ref.set(stateRef, null);
      yield* Queue.offer(restartQueue, void 0); // Signal restart
      yield* Effect.logDebug('Connection closed abruptly!!!');
    });

    const handleShutdown = Effect.gen(function* () {
      yield* handleClose;
      yield* Effect.logDebug('Finalizer: shutting down queues...');
      yield* Queue.shutdown(queue);
      yield* Queue.shutdown(restartQueue);
      yield* Effect.logDebug('Finalizer: shutting down socket...');
      const currentState = yield* Ref.get(stateRef);
      const socket = currentState?.socket;
      if (socket && !socket.closed) {
        socket.removeAllListeners().end();
        if (!socket.destroyed) {
          socket.destroy();
        }
      }
      yield* Effect.logDebug('Finalizer: done.');
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
              runPromise(Queue.offer(queue, new Uint8Array(chunk)));
            });
            socket.on('error', (err) => {
              runPromise(Effect.fail(new TcpConnectionError(err.message)));
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
        Effect.tap(() => Effect.logDebug('Connection established')),

        Effect.tapErrorCause((cause) =>
          Effect.logError('Connection failed', cause),
        ),
      );
    };

    // Create TCP socket
    const initialState = yield* createSocket();

    yield* Ref.set(stateRef, initialState);

    // Restart logic
    /**
     * If multiple errors or closes happen in quick succession, you may queue multiple restarts.
     */
    // TODO: Debounce the restart calls
    const restart: Effect.Effect<void, TcpConnectionError, never> = Effect.gen(
      function* () {
        yield* Effect.logDebug('Restarting connection');
        const currentState = yield* Ref.get(stateRef);
        if (currentState?.isOpen) return; // Already connected
        yield* createSocket().pipe(
          Effect.flatMap((state) => Ref.set(stateRef, state)),
          Effect.retry(Schedule.fibonacci('5 seconds')),
        );
      },
    );

    // Add a mutex for state management
    const stateMutex = yield* Effect.makeSemaphore(1);
    // Wrap state updates
    const withState = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      stateMutex.withPermits(1)(effect).pipe(Effect.withLogSpan('elapsed'));

    const safeRestart = withState(restart);

    const debouncedRestart = pipe(
      safeRestart,
      Effect.delay('3 seconds'),
      // Effect.raceFirst(Effect.never), // Cancels previous restart if new one comes in
      Effect.uninterruptible,
    );

    yield* pipe(
      Stream.fromQueue(restartQueue, { shutdown: true }),
      Stream.tap(() => Effect.logDebug('Queue restarted')),
      Stream.tap(() => debouncedRestart),
      Stream.runDrain,
      Effect.onInterrupt(() =>
        Effect.logDebug('Queue restart fiber interrupted'),
      ),
      Effect.forkDaemon,
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

  const _sendWithRetry = (data: Uint8Array) =>
    pipe(
      Ref.update(bytesSentRef, (n) => n + data.length),
      Effect.zipRight(Metric.incrementBy(bytesSent, data.length)),
      Effect.flatMap(() => client.sendWithRetry(data)),
    );

  // Initial send
  yield* send(data).pipe(Effect.orElse(() => Effect.logError('deu ruim')));

  // Incoming data handling
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

  /**
   * If a forked fiber fails (e.g., the ping fiber), is the error logged and the fiber restarted if needed?
   */
  // TODO: Improvement: Use Effect.forkScoped or add .pipe(Effect.catchAll(...)) to all long-lived fibers to ensure errors are logged and do not silently kill fibers.
  // TODO: Always Handle Errors in Background Fibers. Wrap all long-running background effects with .pipe(Effect.catchAll(Effect.logError)) so errors never kill a fiber silently.

  // Cleanup on exit
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      yield* Effect.logDebug('Program Finalizer');
      yield* Effect.logDebug('Before printing stats');
      yield* printMetrics;
    }),
  );

  yield* Effect.never;
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

export { TcpConfig, TcpConnection, TcpConnectionLive };
