package com.db.logger.timesource.impl;

import java.math.BigInteger;

/**
 * @author cherrus
 *         created 7/20/12 at 4:38 PM
 */
public enum OffsetUtils {
	;

	public static long offset( final Iterable<Entry> entries ) {
		BigInteger sum = BigInteger.ZERO;
		int count = 0;
		for( final Entry entry : entries ) {
			final BigInteger val = BigInteger.valueOf( entry.referenceValue - entry.valueWithOffset );
			sum = sum.add( val );
			count++;
		}
		if( count <= 0 ) {
			throw new IllegalArgumentException( "entries can't be empty" );
		}
		final BigInteger offset = sum.divide( BigInteger.valueOf( count ) );
		//TODO RC: check is there precision loss?
		return offset.longValue();
	}

	public static final class Entry {
		public final long referenceValue;
		public final long valueWithOffset;

		public Entry( final long referenceValue,
		              final long valueWithOffset ) {
			this.referenceValue = referenceValue;
			this.valueWithOffset = valueWithOffset;
		}

		@Override
		public String toString() {
			return String.format(
					"[ref:%d, actual:%d]",
					referenceValue,
					valueWithOffset
			);
		}
	}
}
