package com.db.logger.timesource;

/**
 * Generic interface to abstract out source of time information
 *
 * @author cherrus
 *         created 7/17/12 at 11:02 AM
 */
public interface ITimestampSource {
	/** @return nanoseconds since Jan, 1 1970 GMT (Unix time origin) */
	public long timestampNanos();

	/** @return microseconds since Jan, 1 1970 GMT (Unix time origin) */
	public long timestampMicros();

	/** @return milliseconds since Jan, 1 1970 GMT (Unix time origin) */
	public long timestampMillis();
}
