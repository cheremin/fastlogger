package com.db.logger.api.impl.logger;

import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.buffer.PlainCircularLongsBuffer;

/**
 * @author ruslan
 *         created 22.11.13 at 23:24
 */
public class PlainLongBufferTest extends LongBufferTestBase {
	@Override
	protected ICircularLongsBuffer buffer( final int length ) {
		return new PlainCircularLongsBuffer( length );
	}

	public PlainLongBufferTest( final int length ) {
		super( length );
	}
}
