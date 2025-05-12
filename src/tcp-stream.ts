import { Duration, Effect, Fiber, Queue, Ref, Stream, pipe } from 'effect';

// =========================================================================
// TCP Connection with Write support
// =========================================================================
export interface TcpConnection {
  readonly stream: Stream.Stream<Uint8Array, Error>;
  readonly send: (data: Uint8Array) => Effect.Effect<void>;
  readonly sendText: (data: string) => Effect.Effect<void>;
  readonly close: Effect.Effect<void>;
}

export const createTcpConnection = (options: {
  host: string;
  port: number;
  timeout?: Duration.Duration;
}): Effect.Effect<TcpConnection, Error> => {
  return Effect.scoped(
    Effect.gen(function* () {
      // Create queues for incoming and outgoing data
      const incomingQueue = yield* Queue.unbounded<Uint8Array>();
      const outgoingQueue = yield* Queue.unbounded<Uint8Array>();

      // Track error count
      const writeErrorCount = yield* Ref.make(0);

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

      // Create deferred for connection cleanup
      const bunSocket = yield* Effect.tryPromise(() =>
        Bun.connect({
          port: options.port,
          hostname: options.host,
          socket: {
            data(_socket, data) {
              Queue.unsafeOffer(incomingQueue, data);
            },
            error(_socket, _error) {
              //Queue.unsafeOffer(incomingQueue, error)
              Effect.runPromise(performShutdown);
            },
            close(_socket) {
              Effect.runPromise(performShutdown);
            },
          },
        }),
      ).pipe(
        Effect.timeout(options.timeout ?? Duration.millis(3000)),
        Effect.flatMap((maybeSocket) =>
          maybeSocket
            ? Effect.succeed(maybeSocket)
            : Effect.fail(new Error('Connection timeout')),
        ),
      );

      // Fiber for writing outgoing data
      const writerFiber = yield* pipe(
        Effect.iterate(undefined, {
          while: () => true,
          body: () =>
            pipe(
              Queue.take(outgoingQueue),
              Effect.flatMap((data) =>
                Effect.try({
                  try: () => {
                    const bytesWritten = bunSocket.write(data);
                    if (bytesWritten !== data.length) {
                      throw new Error('Partial write');
                    }
                    // Reset error count on success
                    Effect.runSync(Ref.set(writeErrorCount, 0));
                  },
                  catch: (error) => {
                    const currentErrors = Effect.runSync(
                      Ref.updateAndGet(writeErrorCount, (n) => n + 1),
                    );
                    if (currentErrors > 3) {
                      // Too many errors, close the socket
                      Effect.runSync(performShutdown);
                      return Effect.fail(
                        new Error(
                          `Write failed after ${currentErrors} attempts: ${error}`,
                        ),
                      );
                    }
                    // Retry with the same data after a delay
                    return Effect.sleep(Duration.millis(100)).pipe(
                      Effect.flatMap(() => Queue.offer(outgoingQueue, data)),
                    );
                  },
                }),
              ),
            ),
        }),
        Effect.fork,
      );

      // Cleanup procedure
      const close = Effect.gen(function* () {
        console.log('Closing connection');
        yield* performShutdown;
      });

      // returns TCPConnection
      return {
        stream: Stream.fromQueue(incomingQueue).pipe(Stream.ensuring(close)),
        send: (data: Uint8Array) => Queue.offer(outgoingQueue, data),
        sendText: (data: string) =>
          Queue.offer(outgoingQueue, new TextEncoder().encode(data)),
        close,
      };
    }),
  );
};
