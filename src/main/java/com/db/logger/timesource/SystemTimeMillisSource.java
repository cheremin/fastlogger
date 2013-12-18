package com.db.logger.timesource;


/**
 * @author cherrus
 *         created 7/17/12 at 11:18 AM
 */
public class SystemTimeMillisSource extends BaseTimestampSource {

	public long timestampNanos() {
		return System.currentTimeMillis() * 1000000L;
	}

	@Override
	public String toString() {
		return "System.currentTimeMillis() * 1000000L";
	}
}
