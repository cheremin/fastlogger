package com.db.logger.api.impl.logger;

/**
 * @author ruslan
 *         created 10.12.13 at 1:33
 */
public class SCSDSequencer extends Sequencer {
	public SCSDSequencer( final int length ) {
		super( length );
	}

	@Override
	public long claim( final int size,
	                   final WaitingStrategy waitingStrategy ) {
		final long tail = tailCursor;
		for( int tries = 0; ; tries++ ) {
			final long head = headCursor;
			final long claimedIndexes = tail - head;
			if( claimedIndexes + size <= length ) {
				UNSAFE.putOrderedLong( this, TAIL_OFFSET, tail + size );
				return tail;
			}
			if( !waitingStrategy.waitFor( tries ) ) {
				//we're full now
				return INVALID_INDEX;
			}
		}
	}
}
