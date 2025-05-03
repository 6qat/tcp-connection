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
} from "effect";

import type { TimeoutException } from "effect/Cause";

// Base error class with a more flexible tag system
export class TcpConnectionError extends Data.TaggedError("TcpConnectionError") {
  constructor(readonly errorType?: string) {
    super();
  }
}

// Specific error type that extends the base error
export class BunError extends TcpConnectionError {
  // Override the errorType to identify specific errors
  readonly errorType = "BunError";

  constructor(readonly message: string) {
    super("BunError");
  }
}

export interface TcpStreamConfig {
  readonly host: string;
  readonly port: number;
  readonly bufferSize?: number;
  readonly connectTimeout?: Duration.DurationInput;
}

export class TcpStream {
  private constructor(
    private readonly incomingQueue: Queue.Queue<Chunk.Chunk<Uint8Array>>,
    private readonly outgoingQueue: Queue.Queue<Uint8Array>,
    private readonly performShutdown: Effect.Effect<void>
  ) {}

  static connect(
    config: TcpStreamConfig
  ): Effect.Effect<TcpStream, TcpConnectionError | TimeoutException> {
    return Effect.gen(function* () {
      const incomingQueue = yield* Queue.bounded<Chunk.Chunk<Uint8Array>>(
        config.bufferSize ?? 1024
      );
      const outgoingQueue = yield* Queue.bounded<Uint8Array>(
        config.bufferSize ?? 1024
      );

      // Use refs for coordinated cleanup
      const isClosing = yield* Ref.make(false);

      // Safely shut down once
      const performShutdown = Effect.gen(function* () {
        const alreadyClosing = yield* Ref.getAndSet(isClosing, true);
        if (alreadyClosing) return;

        bunSocket.end();

        // Allow socket events to propagate
        yield* Effect.sleep(Duration.millis(10));

        // Interrupt writer fiber before shutting down queues
        yield* Fiber.interrupt(writerFiber);

        yield* Effect.all([
          Queue.shutdown(incomingQueue),
          Queue.shutdown(outgoingQueue),
        ]);
      });

      const _bunSocket = Effect.tryPromise({
        try: () =>
          Bun.connect({
            port: config.port,
            hostname: config.host,
            socket: {
              data(_socket, data) {
                Effect.runPromise(Queue.offer(incomingQueue, Chunk.of(data)));
              },
              error(_socket, _error) {
                Effect.runPromise(performShutdown);
                throw new BunError("Error on Bun.connect() handler");
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
                error instanceof Error ? error.message : (error as string)
              ),
      }).pipe(
        Effect.timeout(Duration.toMillis(config.connectTimeout ?? "5 seconds")),
        Effect.onInterrupt(() => performShutdown)
      );
      const bunSocket = yield* _bunSocket;

      // Writer fiber
      const _writerFiber = pipe(
        Effect.iterate(undefined, {
          while: () => true,
          body: () =>
            Queue.take(outgoingQueue).pipe(
              Effect.flatMap((data) =>
                data.length === 0
                  ? Effect.void // Backpressure signal
                  : Effect.try({
                      try: () => {
                        const bytesWritten = bunSocket.write(data);
                        if (bytesWritten !== data.length) {
                          throw new BunError("Partial write");
                        }
                      },
                      catch: (error) =>
                        error instanceof BunError
                          ? error
                          : new BunError(
                              error instanceof Error
                                ? error.message
                                : (error as string)
                            ),
                    }).pipe(
                      Effect.retry(
                        Schedule.exponential("100 millis").pipe(
                          Schedule.compose(Schedule.recurs(5))
                        )
                      )
                    )
              )
            ),
        }).pipe(Effect.map(() => undefined)),
        Effect.fork
      );
      const writerFiber = yield* _writerFiber;

      return new TcpStream(incomingQueue, outgoingQueue, performShutdown);
    });
  }

  get incomingStream(): Stream.Stream<Uint8Array, TcpConnectionError> {
    return Stream.fromQueue(this.incomingQueue).pipe(
      Stream.mapChunks(Chunk.flatMap(identity)),
      Stream.catchAll(() => Stream.empty)
    );
  }

  write(data: Uint8Array): Effect.Effect<boolean, TcpConnectionError> {
    return Queue.offer(this.outgoingQueue, data).pipe(
      Effect.catchAll(() =>
        Effect.fail(new Error("Connection closed") as TcpConnectionError)
      ),
      Effect.tap(() => Effect.logDebug(`Sent ${data.length} bytes`))
    );
  }

  writeText(data: string): Effect.Effect<boolean, TcpConnectionError> {
    return this.write(new TextEncoder().encode(data));
  }

  close(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      yield* self.performShutdown;
    });
  }

  static managedStream(
    config: TcpStreamConfig
  ): Stream.Stream<Uint8Array, TcpConnectionError | TimeoutException> {
    return Stream.acquireRelease(TcpStream.connect(config), (stream) =>
      stream.close()
    ).pipe(Stream.flatMap((stream) => stream.incomingStream));
  }
}

// Usage examples
const program = Effect.gen(function* () {
  const stream = yield* TcpStream.connect({
    host: "www.terra.com.br",
    port: 80,
    bufferSize: 4096,
    connectTimeout: "2 seconds",
  });

  yield* stream.writeText("GET / HTTP/1.1\r\nHost: www.terra.com.br\r\n\r\n");

  yield* pipe(
    stream.incomingStream,
    Stream.takeUntil((data) => data.includes(0x04)), // ETX
    Stream.runCollect,
    Effect.tap((chunks) => Effect.log(`Received ${chunks.length} chunks`))
  );

  yield* stream.close();
}).pipe(Effect.catchAll((error) => Effect.logError(error.message)));

Effect.runPromise(program);
