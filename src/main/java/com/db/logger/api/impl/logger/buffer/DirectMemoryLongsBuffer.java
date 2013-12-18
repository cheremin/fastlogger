package com.db.logger.api.impl.logger.buffer;

import java.nio.ByteBuffer;

import com.db.logger.api.impl.logger.UnsafeHelper;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author ruslan
 *         created 11.12.13 at 22:38
 */
public class DirectMemoryLongsBuffer implements ICircularLongsBuffer {

	private final ByteBuffer buffer;

	private final int length;

	private transient final int mask;
	private transient final long address;

	public DirectMemoryLongsBuffer( final ByteBuffer buffer,
	                                final int length ) {
		checkArgument( ( length & ( length - 1 ) ) == 0,
		               "length(%s) must be 2^N", length );
		checkArgument( buffer.isDirect(),
		               "Can't use non-direct buffers" );
		checkArgument( buffer.capacity() >= length,
		               "buffer.capacity(%s) < length(%s)", buffer.capacity(), length );


		this.buffer = buffer;
		this.length = length;

		this.mask = length - 1;
		this.address = ( ( DirectBuffer ) buffer ).address();
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public void put( final long position,
	                 final long value ) {
		final long offset = offset( position );
		UNSAFE.putLong( address + offset, value );
	}

	@Override
	public long get( final long position ) {
		final long offset = offset( position );
		return UNSAFE.getLong( address + offset );
	}

	@Override
	public void putOrdered( final long position,
	                        final long value ) {
		final long offset = offset( position );
		UNSAFE.putOrderedLong( null, address + offset, value );
	}

	@Override
	public void putVolatile( final long position,
	                         final long value ) {
		final long offset = offset( position );
		UNSAFE.putLongVolatile( null, address + offset, value );
	}

	@Override
	public long getVolatile( final long position ) {
		final long offset = offset( position );
		return UNSAFE.getLongVolatile( null, address + offset );
	}

	/*=================== DARK MAGIC =========================*/

	private static final Unsafe UNSAFE = UnsafeHelper.unsafe();
	private static final int INDEX_SHIFT = 3;//sizeof(long) == 2^3

	private long offset( final long position ) {
		return ( ( position & mask ) << INDEX_SHIFT );
	}
}
