import { Effect, pipe, Schedule } from 'effect';

// Send "ping" every second
export const ping = (
  send: (data: Uint8Array) => Effect.Effect<void, never, never>,
) =>
  pipe(
    Effect.repeat(
      pipe(
        Effect.flatMap(
          Effect.sync(() => Buffer.from('ping')),
          (data) =>
            send(data).pipe(
              Effect.catchAll((error) => Effect.logError(error)),
              // Effect.zipRight(printMetrics), // <-- Add this line
            ),
        ),
      ),
      Schedule.spaced('2 seconds'),
    ),
    Effect.fork,
  );

// Retry logic for send
export const sendWithRetry = (
  send: (data: Uint8Array) => Effect.Effect<void, never, never>,
  data: Uint8Array,
) =>
  send(data).pipe(
    Effect.retry(
      Schedule.exponential('100 millis', 2).pipe(
        Schedule.compose(Schedule.recurUpTo(5)),
      ),
    ),
    Effect.catchAll((error) => Effect.logError(error)),
  );
