package com.db.logger.api.impl.logger.buffer;

import java.util.*;

import com.db.logger.api.impl.logger.UnsafeHelper;
import sun.misc.Unsafe;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Same as {@linkplain PlainCircularLongsBuffer}, but uses Unsafe for all accesses,
 * not only synchronization, so bypass bounds checking
 *
 * @author cherrus
 *         created 11/12/13 at 4:43 PM
 */
public class UnsafeCircularLongsBuffer implements ICircularLongsBuffer {

	private final long[] array;
	private final transient int mask;

	public UnsafeCircularLongsBuffer( final int length ) {
		checkArgument(
				( length & ( length - 1 ) ) == 0,
				"length(%s) must be 2^N", length
		);
		this.array = new long[length];
		this.mask = length - 1;
	}

	public UnsafeCircularLongsBuffer( final int length,
	                                  final long fillWithValue ) {
		this( length );
		Arrays.fill( array, fillWithValue );
	}

	@Override
	public int length() {
		return array.length;
	}

	@Override
	public void put( final long position,
	                 final long value ) {
		final long rawIndex = rawIndex( index( position ) );
		UNSAFE.putLong( array, rawIndex, value );
	}

	@Override
	public void putOrdered( final long position,
	                        final long value ) {
		final long rawIndex = rawIndex( index( position ) );
		UNSAFE.putOrderedLong( array, rawIndex, value );
	}

	@Override
	public void putVolatile( final long position,
	                         final long value ) {
		final long rawIndex = rawIndex( index( position ) );
		UNSAFE.putLongVolatile( array, rawIndex, value );
	}

	@Override
	public long get( final long position ) {
		final long rawIndex = rawIndex( index( position ) );
		return UNSAFE.getLong( array, rawIndex );
	}

	@Override
	public long getVolatile( final long position ) {
		final long rawIndex = rawIndex( index( position ) );
		return UNSAFE.getLongVolatile( array, rawIndex );
	}

	/*=================== DARK MAGIC =========================*/

	private static final Unsafe UNSAFE = UnsafeHelper.unsafe();
	private static final int BASE_ARRAY_OFFSET;
	private static final int INDEX_SHIFT;

	static {
		BASE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset( long[].class );
		final int indexScale = UNSAFE.arrayIndexScale( long[].class );
		if( Integer.bitCount( indexScale ) != 1 ) {
			throw new AssertionError( "Array index scale " + indexScale + " must be power of 2" );
		}
		INDEX_SHIFT = Integer.numberOfTrailingZeros( indexScale );
	}

	private static long rawIndex( final int index ) {
		return BASE_ARRAY_OFFSET + ( index << INDEX_SHIFT );
	}

	private int index( final long position ) {
		return ( int ) ( position & mask );
	}
}
