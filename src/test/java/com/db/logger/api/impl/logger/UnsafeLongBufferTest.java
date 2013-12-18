package com.db.logger.api.impl.logger;


import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.buffer.UnsafeCircularLongsBuffer;

/**
 * @author ruslan
 *         created 22.11.13 at 23:24
 */
public class UnsafeLongBufferTest extends LongBufferTestBase {
	@Override
	protected ICircularLongsBuffer buffer( final int length ) {
		return new UnsafeCircularLongsBuffer( length );
	}

	public UnsafeLongBufferTest( final int length ) {
		super( length );
	}
}
