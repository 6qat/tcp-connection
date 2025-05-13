// src/tcp-stream.ts
import {
  Effect,
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
} from 'effect';
import { TimeoutException } from 'effect/Cause';
import { BunRuntime } from '@effect/platform-bun';

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

export interface TcpStreamConfig {
  readonly host: string;
  readonly port: number;
  readonly incomingBufferSize?: number;
  readonly outgoingBufferSize?: number;
  readonly connectTimeout?: Duration.DurationInput;
}

interface TcpStreamShape {
  readonly incomingStream: Stream.Stream<Uint8Array, TcpConnectionError>;
  readonly write: (
    data: Uint8Array,
  ) => Effect.Effect<boolean, TcpConnectionError>;
  readonly writeText: (
    data: string,
  ) => Effect.Effect<boolean, TcpConnectionError>;
  readonly close: Effect.Effect<void>;
  readonly isConnected: Effect.Effect<boolean>;
}

class TcpStream extends Context.Tag('TcpStream')<TcpStream, TcpStreamShape>() {}

const makeEffect = (
  config: TcpStreamConfig,
): Effect.Effect<TcpStreamShape, TcpConnectionError | TimeoutException> =>
  Effect.gen(function* () {
    const incomingQueue = yield* Queue.bounded<Chunk.Chunk<Uint8Array>>(
      config.incomingBufferSize ?? 1024,
    );
    const outgoingQueue = yield* Queue.bounded<Uint8Array>(
      config.outgoingBufferSize ?? 1024,
    );

    // Use refs for coordinated cleanup
    const isClosing = yield* Ref.make(false);

    // Safely shut down once
    const performShutdown = Effect.gen(function* () {
      const alreadyClosing = yield* Ref.getAndSet(isClosing, true);
      if (alreadyClosing) return;
      yield* Effect.logDebug('Shutting down TCP connection');

      bunSocket.end();

      // Allow socket events to propagate
      yield* Effect.sleep(Duration.millis(10));

      // Interrupt writer fiber before shutting down queues
      yield* Fiber.interrupt(writerFiber);

      yield* Effect.all([
        Queue.shutdown(incomingQueue),
        Queue.shutdown(outgoingQueue),
      ]);

      yield* Effect.logDebug('TCP connection shutdown complete');
    });

    const _bunSocket = Effect.tryPromise({
      try: () =>
        Bun.connect({
          port: config.port,
          hostname: config.host,
          socket: {
            open(_socket) {
              Effect.runPromise(
                Effect.logDebug('TCP connection established', {
                  host: config.host,
                  port: config.port,
                }),
              );
            },
            data(_socket, data) {
              Effect.runPromise(Queue.offer(incomingQueue, Chunk.of(data)));
            },
            error(_socket, _error) {
              Effect.runPromise(performShutdown);
              throw new BunError('Error on Bun.connect() handler');
            },
            close(_socket) {
              Effect.runPromise(performShutdown);
            },
          },
        }),
      catch: (error) =>
        error instanceof BunError
          ? error
          : new BunError(
              error instanceof Error ? error.message : 'Unknown error',
            ),
    }).pipe(
      Effect.timeout(Duration.toMillis(config.connectTimeout ?? '5 seconds')),
      Effect.mapError((error) =>
        error instanceof TimeoutException
          ? new TcpConnectionTimeoutError('5 seconds')
          : error,
      ),
      Effect.onInterrupt(() => performShutdown),
    );
    const bunSocket = yield* _bunSocket;

    // Writer fiber
    const _writerFiber = pipe(
      Effect.iterate(undefined, {
        while: () => true,
        body: () =>
          Queue.take(outgoingQueue).pipe(
            Effect.tap((data) =>
              Effect.logDebug(`Dequeuing ${data.length} bytes for writing`),
            ),
            Effect.flatMap((data) =>
              data.length === 0
                ? Effect.void // Backpressure signal
                : Effect.try({
                    try: () => {
                      const bytesWritten = bunSocket.write(data);
                      if (bytesWritten !== data.length) {
                        throw new TcpConnectionWriteError(
                          'Partial write',
                          bytesWritten,
                          data.length,
                        );
                      }
                    },
                    catch: (error) =>
                      error instanceof TcpConnectionError
                        ? error
                        : new BunError(
                            error instanceof Error
                              ? error.message
                              : 'Unknown error.',
                          ),
                  }).pipe(
                    Effect.retry(
                      Schedule.exponential('100 millis').pipe(
                        Schedule.compose(Schedule.recurs(5)),
                      ),
                    ),
                  ),
            ),
          ),
      }).pipe(Effect.map(() => undefined)),
      Effect.fork,
    );
    const writerFiber = yield* _writerFiber;

    const incomingStream = Stream.fromQueue(incomingQueue).pipe(
      Stream.mapChunks(Chunk.flatMap(identity)),
      Stream.catchAll(() => Stream.empty),
    );

    const write = (data: Uint8Array<ArrayBufferLike>) => {
      if (data.length === 0) return Effect.succeed(true);
      return Effect.gen(function* () {
        const closed = yield* Ref.get(isClosing);
        if (closed)
          return yield* Effect.fail(new TcpConnectionError('ConnectionClosed'));

        const success = yield* Queue.offer(outgoingQueue, data);
        yield* Effect.logDebug(`Sent ${data.length} bytes`);
        return success;
      });
    };

    return TcpStream.of({
      incomingStream,
      write,
      writeText: (data) => write(new TextEncoder().encode(data)),
      close: performShutdown,
      isConnected: Effect.gen(function* () {
        const closing = yield* Ref.get(isClosing);
        return !closing;
      }),
    });
  }).pipe(Logger.withMinimumLogLevel(LogLevel.Debug));

const makeLayer = (config: TcpStreamConfig) =>
  Layer.scoped(TcpStream, makeEffect(config));

// Usage examples
const program = Effect.gen(function* () {
  const client = yield* TcpStream;

  yield* client.writeText('GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n');

  yield* pipe(
    client.incomingStream,
    Stream.takeUntil((data) => data.includes(0x04)), // ETX
    Stream.runCollect,
    Effect.tap((chunks) => Effect.log(`Received ${chunks.length} chunks`)),
  );

  yield* client.close;
}).pipe(Effect.catchAll((error) => Effect.logError(error.message)));

BunRuntime.runMain(
  Effect.provide(
    program.pipe(Logger.withMinimumLogLevel(LogLevel.Debug)),
    makeLayer({
      host: 'www.terra.com.br',
      port: 80,
      incomingBufferSize: 4096,
      outgoingBufferSize: 4096,
      connectTimeout: '2 seconds',
    }),
  ),
);

export { TcpStream, makeLayer };
