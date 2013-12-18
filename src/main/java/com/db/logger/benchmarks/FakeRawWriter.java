package com.db.logger.benchmarks;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.db.logger.api.impl.io.RawWriter;

/**
 * @author ruslan
 *         created 04.12.13 at 23:23
 */
public class FakeRawWriter implements RawWriter {
	private final ByteBuffer buffer = ByteBuffer.allocateDirect( 1 << 14 );

	@Override
	public ByteBuffer buffer() {
		return buffer;
	}

	@Override
	public void flush() throws IOException {
		buffer.clear();
	}

	@Override
	public void close() throws IOException {
		buffer.clear();
	}
}
