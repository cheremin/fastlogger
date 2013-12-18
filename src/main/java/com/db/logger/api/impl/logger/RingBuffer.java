package com.db.logger.api.impl.logger;


import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;

/**
 * @author ruslan
 *         created 05.12.13 at 0:41
 */
public final class RingBuffer {
	private final ICircularLongsBuffer buffer;
	private final Sequencer sequencer;
	private final WaitingStrategy waitingStrategy;

	public RingBuffer( final Sequencer sequencer,
	                   final ICircularLongsBuffer buffer,
	                   final WaitingStrategy waitingStrategy ) {
		this.waitingStrategy = waitingStrategy;
		this.sequencer = sequencer;
		this.buffer = buffer;
	}

	public int length() {
		return buffer.length();
	}

	public ICircularLongsBuffer buffer() {
		return buffer;
	}

	public void drainTo( final Sequencer.Drainer drainer ) {
		sequencer.drainTo( drainer );
	}

	public long claim( final int size ) {
		return sequencer.claim(
				size,
				waitingStrategy
		);
	}
}
