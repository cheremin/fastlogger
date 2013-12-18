package com.db.logger.timesource.impl;


import com.db.logger.timesource.BaseTimestampSource;

/**
 * Use {@linkplain System#nanoTime()} as nanoseconds precision timer, and delegate
 * calculation of offset between {@linkplain System#nanoTime()} and unix time origin
 * to {@linkplain com.db.logger.timesource.impl.JDKCombinedTimestampSource.OffsetCalculator}
 * <p/>
 * TODO RC: caution! Currently this implementation is unstable!
 * System.nanoTime() is not guaranteed to be coherent between threads (actually,
 * hardware cores). Per-core timers could be put to sleep as subject of power
 * management, and so de-synchronized. So the use of this class could gives
 * non-accurate results -- non-monotonically increased timestamps, actually.
 * <p/>
 * It it's fine for your algos, keep youself warned, or use {@linkplain PreciseTimestampSource},
 * which is slower, but precise, as it name states.
 *
 * @author cherrus
 *         created 7/20/12 at 3:32 PM
 */
public class JDKCombinedTimestampSource extends BaseTimestampSource {

	private final OffsetCalculator offsetCalculator;

	public JDKCombinedTimestampSource( final OffsetCalculator offsetCalculator ) {
		if( offsetCalculator == null ) {
			throw new IllegalArgumentException( "offsetCalculator can't be null" );
		}
		this.offsetCalculator = offsetCalculator;
	}

	public long timestampNanos() {
		final long systemTimeNanos = System.nanoTime();
		return offsetCalculator.convertToUnix( systemTimeNanos );
	}

	public interface OffsetCalculator {
		/**
		 * Convert value returned by {@linkplain System#nanoTime()} call to Unix time
		 * origin (which is 'Jan, 1, 1970 GMT'). I.e. nanosFromUnixOrigin = convertToUnix(System.nanoTime())
		 */
		public long convertToUnix( final long systemTimeNanos );
	}
}
