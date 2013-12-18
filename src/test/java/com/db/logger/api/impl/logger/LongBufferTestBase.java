package com.db.logger.api.impl.logger;

import java.util.*;

import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * @author ruslan
 *         created 22.11.13 at 23:20
 */
@RunWith( Parameterized.class )
public abstract class LongBufferTestBase {
	protected abstract ICircularLongsBuffer buffer( final int size );


	@Parameterized.Parameters( name = "length = {0}" )
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[] { 2 },
				new Object[] { 4 },
				new Object[] { 8 },
				new Object[] { 16 },
				new Object[] { 32 },
				new Object[] { 128 },
				new Object[] { 1024 },
				new Object[] { 1 << 14 }
		);
	}

	private final int length;

	protected LongBufferTestBase( final int length ) {
		this.length = length;
	}

	@Test( expected = IllegalArgumentException.class )
	public void lengthMustBePowerOfTwo() throws Exception {
		buffer( length + 1 );
	}

	@Test
	public void valueWrittenCouldBeRead() throws Exception {
		final ICircularLongsBuffer buffer = buffer( length );
		for( long pos = 0; pos < length; pos++ ) {
			buffer.put( pos, pos );
		}
		for( long pos = 0; pos < length; pos++ ) {
			assertEquals(
					pos,
					buffer.get( pos )
			);
		}
	}

	@Test
	public void valueWrittenCouldBeReadCyclic() throws Exception {
		final ICircularLongsBuffer buffer = buffer( length );
		for( long pos = 0; pos < length * 4; pos++ ) {
			buffer.put( pos, pos );
			assertEquals(
					pos,
					buffer.get( pos )
			);
		}
	}

	@Test
	public void valueWrittenOrderedCouldBeRead() throws Exception {
		final ICircularLongsBuffer buffer = buffer( length );
		for( long pos = 0; pos < length; pos++ ) {
			buffer.putOrdered( pos, pos );
		}
		for( long pos = 0; pos < length; pos++ ) {
			assertEquals(
					pos,
					buffer.get( pos )
			);
		}
	}

	@Test
	public void valueWrittenOrderedCouldBeReadVolatile() throws Exception {
		final ICircularLongsBuffer buffer = buffer( length );
		for( long pos = 0; pos < length; pos++ ) {
			buffer.putOrdered( pos, pos );
		}
		for( long pos = 0; pos < length; pos++ ) {
			assertEquals(
					pos,
					buffer.getVolatile( pos )
			);
		}
	}

	@Test
	public void valueWrittenCouldBeReadVolatile() throws Exception {
		final ICircularLongsBuffer buffer = buffer( length );
		for( long pos = 0; pos < length; pos++ ) {
			buffer.put( pos, pos );
		}
		for( long pos = 0; pos < length; pos++ ) {
			assertEquals(
					pos,
					buffer.getVolatile( pos )
			);
		}
	}
}
