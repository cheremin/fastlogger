package com.db.logger.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.db.logger.api.impl.logger.*;
import com.db.logger.api.impl.logger.buffer.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

import static com.db.logger.api.impl.logger.RecordHelper.*;
import static com.db.logger.api.impl.logger.RecordHelper.RecordType.LOG_RECORD;

/**
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class ArrayWriteAndDrainBenchmark {
	public static final int LENGTH_POW = Integer.getInteger( "length-pow", 14 );
	public static final int CELLS_PER_RECORD = Integer.getInteger( "cells-per-record", 8 );//8longs = 1 cache line
	public static final int PAYLOAD = Integer.getInteger( "writer-backoff", 20 );
	public static final int LENGTH = 1 << LENGTH_POW;
	public static final int MASK = LENGTH - 1;

	public static final WaitingStrategy WAITING_STRATEGY = new WaitingStrategy.LimitedSpinning( 1024 * 128 );

	public static final AtomicInteger ID_GENERATOR = new AtomicInteger( 1 );

	public static final Sequencer.Drainer DRAIN_DUMMY = new Sequencer.Drainer() {
		@Override
		public int available( final long startSequence,
		                      final long sentinelSequence ) {
			return ( int ) ( sentinelSequence - startSequence );
		}
	};

	static {
		System.out.printf( "len=2^%d, record=%d, payload=%d\n", LENGTH_POW, CELLS_PER_RECORD, PAYLOAD );
	}

	public ILongsArray buffer;
	public MCSDSequencer sequencer;

	public Sequencer.Drainer DRAIN_AND_READ;

	@Setup
	public void setup() {
		buffer = new UnsafeLongsArray( LENGTH, NOT_SET );
		sequencer = new MCSDSequencer( LENGTH );

		DRAIN_AND_READ = new ConsumingDrainer( buffer );
	}

	@TearDown
	public void tearDown() {

	}

	@Setup( Level.Iteration )
	public void attachThread() {
		//TODO set thread affinity
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "payload" )
	@GroupThreads( 4 )
	public void payload() {
		BlackHole.consumeCPU( PAYLOAD );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDummyDrain" )
	@GroupThreads( 3 )//actually it's (CORES-1)
	public void writer( final ThreadState ts ) {
		writeEntry( ts.id, ts.count );
		payload();
	}

	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDummyDrain" )
	@GroupThreads( 1 )
	public void dummyDrainer() {
		//mostly we do not care about drain latency here, measure just to be aware of it
		try {
			sequencer.drainTo( DRAIN_DUMMY );
			BlackHole.consumeCPU( PAYLOAD / 3 );
		} catch( Throwable e ) {
			e.printStackTrace();
		}
	}

	/*=============================================================================*/

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDrainAndRead" )
	@GroupThreads( 3 ) //actually it's (CORES-1)
	public void writer2( final ThreadState ts ) {
		writeEntry( ts.id, ts.count );
		payload();
	}

	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDrainAndRead" )
	@GroupThreads( 1 )
	public void readingDrainer() {
		//mostly we do not care about drain latency here, measure just to be aware of it
		try {
			sequencer.drainTo( DRAIN_AND_READ );
			BlackHole.consumeCPU( PAYLOAD / 3 );
		} catch( Throwable e ) {
			e.printStackTrace();
		}
	}

	/*=============================================================================*/

	private void writeEntry( final int writerId,
	                         final int cellsCount ) {
		final long position = sequencer.claim( cellsCount, WAITING_STRATEGY );
		if( position < 0 ) {
			System.err.println( "Timeout" );
			return;
		}

		final int argumentsCount = cellsCount - 1;

		final int headerIndex = index( position );
		if( headerIndex + argumentsCount < LENGTH ) {
			//reserve 0-th cell for header
			for( int i = 1; i < cellsCount; i++ ) {
				final int index = headerIndex + i;
				buffer.put( index, i );
			}
		} else {
			//reserve 0-th cell for header
			for( int i = 1; i < cellsCount; i++ ) {
				final int index = index( position + i );
				buffer.put( index, i );
			}
		}


		//write header with SA
		final long header = header( LOG_RECORD, writerId, argumentsCount );
		buffer.putOrdered( headerIndex, header );
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();
		public final int count = CELLS_PER_RECORD;//but 1 for header
	}

	public static void main( final String[] args ) throws Exception {
		final ArrayWriteAndDrainBenchmark benchmark = new ArrayWriteAndDrainBenchmark();

		benchmark.setup();

		final ThreadState[] states = new ThreadState[3];
		for( int i = 0; i < states.length; i++ ) {
			final ThreadState state = new ThreadState();
			states[i] = state;
			new Thread() {
				@Override
				public void run() {
					for(; ; ) {
						benchmark.writer2( state );
					}
				}
			}.start();
		}

		new Thread() {
			@Override
			public void run() {
				for(; ; ) {
					benchmark.readingDrainer();
				}
			}
		}.start();

		Thread.sleep( 300000 );
	}

	public class ConsumingDrainer implements Sequencer.Drainer {
		private static final int SPINS_PER_TURN = 256;

		private final ILongsArray buffer;
		private final BlackHole hole = new BlackHole();
		private transient final int mask;

		public ConsumingDrainer( final ILongsArray buffer ) {
			this.buffer = buffer;
			this.mask = buffer.length() - 1;
		}

		private int spinsAvailable;

		@Override
		public int available( final long startSequence,
		                      final long sentinelSequence ) {
			spinsAvailable = SPINS_PER_TURN;
			for( long pos = startSequence; pos < sentinelSequence; pos++ ) {
				final int headerIndex = index( pos );
				final long header = readHeader( headerIndex );
				if( !isValidHeader( header ) ) {
					return ( int ) ( pos - startSequence );
				}
				final RecordHelper.RecordType type = type( header );
				final int formatId = formatId( header );
				final int argumentsCount = argumentsCount( header );

				buffer.put( headerIndex, NOT_SET );
				if( headerIndex + argumentsCount < buffer.length() ) {
					for( int i = 1; i <= argumentsCount; i++ ) {
						final int index = headerIndex + i;
						final long arg = buffer.get( index );
						buffer.put( index, NOT_SET );//needs to reclaim all!
						hole.consume( arg );
					}
				} else {
					for( int i = 1; i <= argumentsCount; i++ ) {
						final int index = index( pos + i );
						final long arg = buffer.get( index );
						buffer.put( index, NOT_SET );//needs to reclaim all!
						hole.consume( arg );
					}
				}

				pos += argumentsCount;
			}
			return ( int ) ( sentinelSequence - startSequence );
		}

		private int index( final long position ) {
			return ( ( int ) position ) & mask;
		}

		private long readHeader( final int index ) {
			for(; spinsAvailable >= 0; spinsAvailable-- ) {
				final long header = buffer.getVolatile( index );
				if( isValidHeader( header ) ) {
					return header;
				}
			}
			return NOT_SET;
		}
	}

	private static int index( final long position ) {
		return ( ( int ) position ) & MASK;
	}
}
