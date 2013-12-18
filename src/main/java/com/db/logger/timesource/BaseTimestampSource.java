package com.db.logger.timesource;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Base implementation -- left only {@linkplain #timestampNanos()} methods
 * left to be implemented
 *
 * @author cherrus
 *         created 9/13/12 at 11:19 AM
 */
public abstract class BaseTimestampSource implements ITimestampSource {
	public long timestampMicros() {
		final long nanos = timestampNanos();
		return NANOSECONDS.toMicros( nanos );
	}

	public long timestampMillis() {
		final long nanos = timestampNanos();
		return NANOSECONDS.toMillis( nanos );
	}
}
