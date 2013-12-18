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
public interface ILongsArray {

	/** @return in longs (8 bytes) */
	public int length();

	public void put( final int index,
	                 final long value );

	public void putOrdered( final int index,
	                        final long value );

	public void putVolatile( final int index,
	                         final long value );

	public long get( final int index );

	public long getVolatile( final int index );
}
