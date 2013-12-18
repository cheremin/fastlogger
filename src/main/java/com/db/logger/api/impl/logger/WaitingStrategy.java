package com.db.logger.api.impl.logger;

/**
 * @author ruslan
 *         created 22.11.13 at 0:46
 */
public interface WaitingStrategy {
	public static final WaitingStrategy NO_WAIT = new WaitingStrategy() {
		@Override
		public boolean waitFor( final int tries ) {
			return false;
		}
	};
	public static final WaitingStrategy SPINNING = new WaitingStrategy() {
		@Override
		public boolean waitFor( final int tries ) {
			for( int i = 0; i < tries; i++ ) {

			}
			return true;
		}
	};


	public boolean waitFor( final int tries );

	public static class LimitedSpinning implements WaitingStrategy {
		private final int maxCount;

		public LimitedSpinning( final int maxCount ) {
			this.maxCount = maxCount;
		}

		public static volatile long consumedCPU = 43;

		@Override
		public boolean waitFor( final int tries ) {
			if( tries >= maxCount ) {
				return false;
			}
			// randomize start so that JIT could not memoize;
			long t = consumedCPU;

			for( long i = 0; i < tries; i++ ) {
				t += ( t * 0x5DEECE66DL + 0xBL ) & ( 0xFFFFFFFFFFFFL );
			}

			// need to guarantee side-effect on the result,
			// but can't afford contention; make sure we update the shared state
			// only in the unlikely case, so not to do the furious writes,
			// but still dodge DCE.
			if( t == 42 ) {
				consumedCPU += t;
			}
			return true;
		}
	}
}
