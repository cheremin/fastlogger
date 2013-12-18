package com.db.logger.api.impl.logger.buffer;

import java.util.*;

import com.db.logger.api.impl.logger.UnsafeHelper;
import sun.misc.Unsafe;


/**
 * Same as {@linkplain com.db.logger.api.impl.logger.buffer.PlainCircularLongsBuffer}, but uses Unsafe for all accesses,
 * not only synchronization, so bypass bounds checking
 *
 * @author cherrus
 *         created 11/12/13 at 4:43 PM
 */
public class UnsafeLongsArray implements ILongsArray {

	private final long[] array;

	public UnsafeLongsArray( final int length ) {
		this.array = new long[length];
	}

	public UnsafeLongsArray( final int length,
	                         final long fillWithValue ) {
		this( length );
		Arrays.fill( array, fillWithValue );
	}

	@Override
	public int length() {
		return array.length;
	}

	@Override
	public void put( final int index,
	                 final long value ) {
		final long rawIndex = rawIndex( index );
		UNSAFE.putLong( array, rawIndex, value );
	}

	@Override
	public void putOrdered( final int index,
	                        final long value ) {
		final long rawIndex = rawIndex( index );
		UNSAFE.putOrderedLong( array, rawIndex, value );
	}

	@Override
	public void putVolatile( final int index,
	                         final long value ) {
		final long rawIndex = rawIndex( index );
		UNSAFE.putLongVolatile( array, rawIndex, value );
	}

	@Override
	public long get( final int index ) {
		final long rawIndex = rawIndex( index );
		return UNSAFE.getLong( array, rawIndex );
	}

	@Override
	public long getVolatile( final int index ) {
		final long rawIndex = rawIndex( index );
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
}
