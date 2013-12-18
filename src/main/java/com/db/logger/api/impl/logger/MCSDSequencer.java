package com.db.logger.api.impl.logger;


/**
 * [Fixed capacity circular buffer]'s index maintainer. It is multithreaded data
 * structure for data flows de-multiplexing. It allows several threads to claim
 * record index (the actual records storage is out of scope of the class), and <b>one
 * thread</b> to periodically query claimed index range and reclaim indexes for reuse.
 * <p/>
 * Client code must involve some kind of protocol to ensure that entry filling with
 * data was finished, since sequencer itself takes responsibility only on spreading
 * indexes to producers. {@link #drainTo(MCSDSequencer.Drainer)}
 * will supply all indexes which was given by {@link #claim} at moment,
 * but not all of associated records are full with data. You need some kind of flag
 * in record for this
 *
 * @author cherrus
 *         created 7/4/12 at 3:54 PM
 */
public class MCSDSequencer extends Sequencer {

	public MCSDSequencer( final int length ) {
		super( length );
	}


	@Override
	public long claim( final int size,
	                   final WaitingStrategy waitingStrategy ) {
		for( int tries = 0; ; tries++ ) {
			final long head = headCursor;
			final long tail = tailCursor;
			final long claimedIndexes = tail - head;
			if( claimedIndexes + size > length ) {
				if( !waitingStrategy.waitFor( tries ) ) {
					//we're full now
					return INVALID_INDEX;
				}
			} else if( UNSAFE.compareAndSwapLong( this, TAIL_OFFSET, tail, tail + size ) ) {
				return tail;
			}
			//once more try
		}
	}
}
