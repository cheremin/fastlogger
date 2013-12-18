package com.db.logger.benchmarks.helpers;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.db.logger.api.impl.logger.MCSDSequencer;
import com.db.logger.api.impl.logger.SCSDSequencer;
import com.db.logger.api.impl.logger.WaitingStrategy;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

/**
 * Here we measure raw sequencer performance: just position claiming/reclaiming
 * It is single-claimer single-drainer (SCSD) sequencer
 * <p/>
 *
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class SCSDSequencerBenchmark {
	private static final Logger log = Logger.getLogger( SCSDSequencerBenchmark.class );

	public static final int LENGTH_POW = Integer.getInteger( "length-pow", 14 );
	public static final int LENGTH = 1 << LENGTH_POW;
	public static final int CELLS_PER_RECORD = Integer.getInteger( "record-size", 8 );//8longs = 1 cache line
	public static final int PAYLOAD = Integer.getInteger( "writer-backoff", 0 );

	public static final WaitingStrategy WAITING_STRATEGY = new WaitingStrategy.LimitedSpinning( 1024 * 128 );

	static {
		BasicConfigurator.configure();
		System.out.printf( "len=%d, record=%d, payload=%d\n", LENGTH, CELLS_PER_RECORD, PAYLOAD );
	}

	public static final AtomicInteger ID_GENERATOR = new AtomicInteger( 1 );

	public static final MCSDSequencer.Drainer DRAIN_DUMMY = new MCSDSequencer.Drainer() {
		@Override
		public int available( final long startSequence,
		                      final long sentinelSequence ) {
			return ( int ) ( sentinelSequence - startSequence );
		}
	};


	public static final List<ThreadState> sequencers = new CopyOnWriteArrayList<ThreadState>();

	@Setup
	public void setup() {

	}

	@TearDown
	public void tearDown() {
		sequencers.clear();
	}

	@Setup( Level.Iteration )
	public void attachThread() {
		//TODO set thread affinity
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "payload" )
	@GroupThreads( 4 )
	public void backoffAlone() {
		BlackHole.consumeCPU( PAYLOAD );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "claim3AndPayloadAndDrain1Dummy" )
	@GroupThreads( 3 )//actually it's (CORES-1)
	public void claimer3( final ThreadState ts ) {
		claimEntry( ts.sequencer, ts.id, ts.count );
		BlackHole.consumeCPU( PAYLOAD );
	}

	@GenerateMicroBenchmark
	@Group( "claim3AndPayloadAndDrain1Dummy" )
	@GroupThreads( 1 )
	public void drainer() {
		for( final ThreadState ts : sequencers ) {
			ts.sequencer.drainTo( DRAIN_DUMMY );
		}
//		BlackHole.consumeCPU( 0 );
	}

	/*=============================================================================*/

	private long claimEntry( final SCSDSequencer sequencer,
	                         final int writerId,
	                         final int cellsCount ) {
		return sequencer.claim( cellsCount, WAITING_STRATEGY );
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();
		public final int count = CELLS_PER_RECORD;

		public SCSDSequencer sequencer;

		@Setup
		public void setup() {
			sequencer = new SCSDSequencer( LENGTH );
			sequencers.add( this );
		}

		@TearDown
		public void destroy() {
			sequencers.remove( this );
		}
	}

	public static void main( final String[] args ) throws Exception {
		final SCSDSequencerBenchmark benchmark = new SCSDSequencerBenchmark();

		benchmark.setup();

		final int BATCH = 10000000;
		final ThreadState[] states = new ThreadState[3];
		for( int i = 0; i < states.length; i++ ) {
			final ThreadState state = new ThreadState();
			states[i] = state;
			new Thread() {
				@Override
				public void run() {
					state.setup();
					for( int turn = 0; ; turn += BATCH ) {
						for( int i = 0; i < BATCH; i++ ) {
							benchmark.claimer3( state );
						}
						log.info( "Thread " + state.id + ": " + turn );
					}
//					state.destroy();
				}
			}.start();
		}

		new Thread() {
			@Override
			public void run() {

				for(; ; ) {
					benchmark.drainer();
				}
			}
		}.start();

		Thread.sleep( 300000 );
	}
}
