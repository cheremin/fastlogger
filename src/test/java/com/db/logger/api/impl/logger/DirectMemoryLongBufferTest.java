package com.db.logger.api.impl.logger;

import java.nio.ByteBuffer;

import com.db.logger.api.impl.logger.buffer.DirectMemoryLongsBuffer;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;

/**
 * @author ruslan
 *         created 22.11.13 at 23:24
 */
public class DirectMemoryLongBufferTest extends LongBufferTestBase {
	public DirectMemoryLongBufferTest( final int length ) {
		super( length );
	}

	@Override
	protected ICircularLongsBuffer buffer( final int length ) {
		return new DirectMemoryLongsBuffer(
				ByteBuffer.allocateDirect( length * 8 ),
				length
		);
	}
}
