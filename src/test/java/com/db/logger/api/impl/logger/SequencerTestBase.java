package com.db.logger.api.impl.logger;

import org.junit.Test;

import static com.db.logger.api.impl.logger.Sequencer.INVALID_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * @author ruslan
 *         created 10.12.13 at 1:57
 */
public abstract class SequencerTestBase {
	protected abstract Sequencer createSequencer( final int size );

	private static void assertExactIndexesAvailable( final Sequencer sequencer,
	                                                 final int expectedAvailable ) {
		for( int i = 0; i < expectedAvailable; i++ ) {
			assertNotEquals( i + " from " + expectedAvailable + " expected",
			                 INVALID_INDEX,
			                 sequencer.claim()
			);
		}
		assertEquals(
				"More entries, then expected(" + expectedAvailable + ")",
				INVALID_INDEX,
				sequencer.claim()
		);
	}

	@Test( expected = IllegalArgumentException.class )
	public void cantCreateEmptySequencer() throws Exception {
		new MCSDSequencer( 0 );
	}

	@Test
	public void sequencerSizeIndexesAvailableInFreshSequencer() throws Exception {
		final int size = 4;
		final Sequencer sequencer = createSequencer( size );
		for( int i = 0; i < size; i++ ) {
			assertNotEquals( INVALID_INDEX, sequencer.claim() );
		}
	}

	@Test
	public void nextAvailableReturnsInvalidIndexWhenSequencerExhausted() throws Exception {
		final int size = 4;
		final Sequencer sequencer = createSequencer( size );

		assertExactIndexesAvailable( sequencer, size );
	}

	@Test
	public void drainerNotCalledIfNothingClaimedYet() throws Exception {
		final int size = 4;
		final Sequencer sequencer = createSequencer( size );
		sequencer.drainTo( new MCSDSequencer.Drainer() {
			@Override
			public int available( final long startSequence,
			                      final long sentinelSequence ) {
				fail( "Called with " + startSequence + ", " + sentinelSequence );
				return 0;
			}
		} );
	}

	@Test
	public void drainToSuppliesEveryIndexInClaimedRange() throws Exception {
		final int size = 4;
		final Sequencer sequencer = createSequencer( size );
		exhaustSequencer( sequencer, size );
		sequencer.drainTo( new MCSDSequencer.Drainer() {
			@Override
			public int available( final long startSequence,
			                      final long sentinelSequence ) {
				assertEquals( 0, startSequence );
				assertEquals( size, sentinelSequence );
				return 0;
			}
		} );

	}

	@Test
	public void drainMakesEntriesAvailableForReuse() throws Exception {
		final int size = 4;

		final Sequencer sequencer = createSequencer( size );

		exhaustSequencer( sequencer, size );

		sequencer.drainTo( new MCSDSequencer.Drainer() {
			@Override
			public int available( final long startSequence,
			                      final long sentinelSequence ) {
				return ( int ) ( sentinelSequence - startSequence );
			}
		} );

		assertExactIndexesAvailable( sequencer, size );
	}

	@Test( expected = IllegalStateException.class )
	public void drainFailsIfReturnedNegativeValue() throws Exception {
		final int size = 4;

		final Sequencer sequencer = createSequencer( size );

		exhaustSequencer( sequencer, size );

		sequencer.drainTo( new MCSDSequencer.Drainer() {
			@Override
			public int available( final long startSequence,
			                      final long sentinelSequence ) {
				return -1;
			}
		} );
	}

	@Test( expected = IllegalStateException.class )
	public void drainFailsIfReturnedLengthMoreThenMaximum() throws Exception {
		final int size = 4;

		final Sequencer sequencer = createSequencer( size );

		exhaustSequencer( sequencer, size );

		sequencer.drainTo( new MCSDSequencer.Drainer() {
			@Override
			public int available( final long startSequence,
			                      final long sentinelSequence ) {
				final long length = sentinelSequence - startSequence;
				return ( int ) ( length + 1 );
			}
		} );
	}

	@Test
	public void drainToMakesAvailableToReuseOnlyDrainedIndexes() throws Exception {
		final int size = 4;

		final Sequencer sequencer = createSequencer( size );

		exhaustSequencer( sequencer, size );

		final int toProcess = 2;

		sequencer.drainTo( new MCSDSequencer.Drainer() {
			@Override
			public int available( final long startSequence,
			                      final long sentinelSequence ) {
				return toProcess;
			}
		} );

		assertExactIndexesAvailable( sequencer, toProcess );
	}

	@Test
	public void exceptionInProcessorJustStopsProcessingAndChangeNothing() throws Exception {
		final int size = 4;

		final Sequencer sequencer = createSequencer( size );

		exhaustSequencer( sequencer, size );

		try {
			sequencer.drainTo( new MCSDSequencer.Drainer() {
				@Override
				public int available( final long startSequence,
				                      final long sentinelSequence ) {
					throw new RuntimeException();
				}
			} );
			fail( "Exception was suppressed!" );
		} catch( RuntimeException e ) {
		}

		assertExactIndexesAvailable( sequencer, 0 );
	}

	private void exhaustSequencer( final Sequencer sequencer,
	                               final int size ) {
		for( int i = 0; i < size; i++ ) {
			sequencer.claim();
		}
	}
}
