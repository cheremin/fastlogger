package com.db.logger.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.db.logger.api.impl.logger.*;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.buffer.UnsafeCircularLongsBuffer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

import static com.db.logger.api.impl.logger.RecordHelper.NOT_SET;
import static com.db.logger.api.impl.logger.RecordHelper.RecordType.LOG_RECORD;
import static com.db.logger.api.impl.logger.RecordHelper.header;

/**
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class BufferWriteAndDrainBenchmark {
	public static final int LENGTH_POW = Integer.getInteger( "length-pow", 14 );
	public static final int CELLS_PER_RECORD = Integer.getInteger( "cells-per-record", 8 );//8longs = 1 cache line
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 20 );

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
		System.out.printf( "len=2^%d, record=%d, payload=%d\n", LENGTH_POW, CELLS_PER_RECORD, WRITER_BACKOFF );
	}

	public ICircularLongsBuffer buffer;
	public MCSDSequencer sequencer;

	public Sequencer.Drainer DRAIN_AND_READ;

	@Setup
	public void setup() {
		final int length = 1 << LENGTH_POW;
		buffer = new UnsafeCircularLongsBuffer( length, NOT_SET );
		sequencer = new MCSDSequencer( length );

		DRAIN_AND_READ = new ConsumingDrainer( buffer, 1 );
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
	public void backoffAlone() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDummyDrain" )
	@GroupThreads( 3 )//actually it's (CORES-1)
	public void writer( final ThreadState ts ) {
		writeEntry( ts.id, ts.count );
		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDummyDrain" )
	@GroupThreads( 1 )
	public void dummyDrainer() {
		//mostly we do not care about drain latency here, measure just to be aware of it
		try {
			sequencer.drainTo( DRAIN_DUMMY );
			BlackHole.consumeCPU( 10 );
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
		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDrainAndRead" )
	@GroupThreads( 1 )
	public void readingDrainer() {
		//mostly we do not care about drain latency here, measure just to be aware of it
		try {
			sequencer.drainTo( DRAIN_AND_READ );
//			BlackHole.consumeCPU( 100 );
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
		//reserve 0-th cell for header
		for( int i = 1; i < cellsCount; i++ ) {
			buffer.put( position + i, i );
		}
		//write header with SA
		final long header = header( LOG_RECORD, writerId, cellsCount - 1 );
		buffer.putOrdered( position, header );
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();
		public final int count = CELLS_PER_RECORD;//but 1 for header
	}

	public static void main( final String[] args ) throws Exception {
		final BufferWriteAndDrainBenchmark benchmark = new BufferWriteAndDrainBenchmark();

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
}
