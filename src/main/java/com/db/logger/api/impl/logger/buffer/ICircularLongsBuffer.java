package com.db.logger.api.impl.logger.buffer;

/**
 * Array of longs with additional synchronization accessors
 * <p/>
 * Also, uses long for indexing, incapsulating position -> index mapping
 * inside.
 *
 * @author cherrus
 *         created 11/12/13 at 4:28 PM
 */
public interface ICircularLongsBuffer {

	/** @return in longs (8 bytes) */
	public int length();

	public void put( final long position,
	                 final long value );

	public void putOrdered( final long position,
	                        final long value );

	public void putVolatile( final long position,
	                         final long value );

	public long get( final long position );

	public long getVolatile( final long position );
}
