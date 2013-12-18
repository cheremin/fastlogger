package com.db.logger.api.impl.logger;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * @author ruslan
 *         created 10.12.13 at 1:26
 */
public abstract class Sequencer {
	public static final long INVALID_INDEX = -1L;

	protected static final Unsafe UNSAFE = UnsafeHelper.unsafe();
	protected static final long HEAD_OFFSET;
	protected static final long TAIL_OFFSET;

	static {
		try {
			final Field headCursorField = Sequencer.class.getDeclaredField( "headCursor" );
			final Field tailCursorField = Sequencer.class.getDeclaredField( "tailCursor" );
			HEAD_OFFSET = UNSAFE.objectFieldOffset( headCursorField );
			TAIL_OFFSET = UNSAFE.objectFieldOffset( tailCursorField );
			final long padding = TAIL_OFFSET - HEAD_OFFSET;
			if( padding < 64 ) {
				System.out.println( "Padding is optimized out: " + padding );
			}
		} catch( NoSuchFieldException e ) {
			throw new RuntimeException( e );
		}
	}

	protected final int length;
	/**
	 * Elements range: [headCursor, tailCursor)
	 * <p/>
	 * (tailCursor - headCursor) == elements count
	 * <p/>
	 * 0 <= (tailCursor - headCursor) <= length  => state invariant
	 * <p/>
	 * tailCursor - headCursor == length         => buffer is full
	 * tailCursor - headCursor == 0              => buffer is empty
	 * <p/>
	 * (headCursor % size ) is the index of first item in buffer
	 * (tailCursor % size ) is the index of _cell_ for _next last item_
	 */
	public volatile long r0;
	public volatile long r1;
	public volatile long r2;
	public volatile long r3;
	public volatile long r4;
	public volatile long r5;
	public volatile long r6;
	public volatile long r7;
	protected volatile long headCursor = 0;
	public volatile long p0;
	public volatile long p1;
	public volatile long p2;
	public volatile long p3;
	public volatile long p4;
	public volatile long p5;
	public volatile long p6;
	public volatile long p7;
	public volatile long q0;
	public volatile long q1;
	public volatile long q2;
	public volatile long q3;
	public volatile long q4;
	public volatile long q5;
	public volatile long q6;
	public volatile long q7;
	protected volatile long tailCursor = 0;
	public volatile long z0;
	public volatile long z1;
	public volatile long z2;
	public volatile long z3;
	public volatile long z4;
	public volatile long z5;
	public volatile long z6;
	public volatile long z7;

	protected Sequencer( final int length ) {
		checkArgument( length > 0, "length(%s) must be > 0", length );
		checkArgument( ( length & ( length - 1 ) ) == 0,
		               "length %s should be power of 2", length );
		this.length = length;
	}

	/** @return -1, if not available */
	public long claim() {
		return claim( 1 );
	}

	public long claim( final int size ) {
		return claim( size, WaitingStrategy.NO_WAIT );
	}

	public abstract long claim( final int size,
	                            final WaitingStrategy waitingStrategy );

	/**
	 * It is single-threaded method: it must be called from one thread, or be
	 * protected by external mutex.
	 */
	public void drainTo( final Drainer drainer ) {
		checkArgument( drainer != null, "drainer can't be null" );

		//remember: claimed indexes are all in [headCursor, tailCursor)
		final long firstClaimed = headCursor;
		final long sentinelIndex = tailCursor;
		if( sentinelIndex > firstClaimed ) {
			final long reclaimedIndexes = drainer.available(
					firstClaimed,
					sentinelIndex
			);
			final long maxForReclaim = sentinelIndex - firstClaimed;
			checkState( reclaimedIndexes >= 0 && reclaimedIndexes <= maxForReclaim,
			            "Can't reclaim %s indexes: only [0,%s] available",
			            reclaimedIndexes, maxForReclaim
			);
			UNSAFE.putOrderedLong( this, HEAD_OFFSET, firstClaimed + reclaimedIndexes );
		}
	}

	public interface Drainer {
		/**
		 * Sequence range [startSequence, sentinelSequence) is claimed by sequencer,
		 * and may be reclaimed
		 *
		 * @return number of sequences reclaimed. I.e [startSequence, startSequence+length]
		 *         would be available for reuse. Max value allowed to return is
		 *         (sentinelSequence-startSequence)
		 */

		public int available( final long startSequence,
		                      final long sentinelSequence );
	}
}
