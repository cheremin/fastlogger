package com.db.logger.timesource.impl;

import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Use {@linkplain System#currentTimeMillis()} for calibrating time origin
 *
 * @author cherrus
 *         created 7/20/12 at 3:48 PM
 */
public class SynchronousJDKOffsetCalculator implements JDKCombinedTimestampSource.OffsetCalculator {

	private final long offset;

	{
		final List<OffsetUtils.Entry> measures = new ArrayList<OffsetUtils.Entry>( 31 );
		try {
			for( int i = 0; i < 31; i++ ) {
				final long nanos = System.nanoTime();
				final long millis = System.currentTimeMillis();
				final OffsetUtils.Entry measure = new OffsetUtils.Entry(
						MILLISECONDS.toNanos( millis ),
						nanos
				);
				measures.add( measure );
				Thread.sleep( i );
			}
		} catch( InterruptedException e ) {
//			log.error( "Error", e );
		}
		offset = OffsetUtils.offset( measures );
	}

	public long convertToUnix( final long systemTimeNanos ) {
		return offset + systemTimeNanos;
	}
}
