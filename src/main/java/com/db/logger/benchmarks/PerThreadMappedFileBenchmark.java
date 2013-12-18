package com.db.logger.benchmarks;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.db.logger.api.impl.logger.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

/**
 * We use many (3) single-writer single-reader ring buffers, one per each
 * 'logging' thread, instead of one multi-writer single reader ring buffer
 * for all them at once.
 *
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class PerThreadMappedFileBenchmark {
	public static final int LENGTH_POW = Integer.getInteger( "length-pow", 14 );
	public static final int CELLS_PER_RECORD = Integer.getInteger( "cells-per-record", 8 );//8longs = 1 cache line
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 20 );

	public static final WaitingStrategy WAITING_STRATEGY = new WaitingStrategy.LimitedSpinning( 1024 * 128 );

	public static final AtomicInteger ID_GENERATOR = new AtomicInteger( 1 );

	public static final Sequencer.Drainer DRAIN_DUMMY = new MCSDSequencer.Drainer() {
		@Override
		public int available( final long startSequence,
		                      final long sentinelSequence ) {
			return ( int ) ( sentinelSequence - startSequence );
		}
	};

	static {
		System.out.printf( "len=2^%d, record=%d, payload=%d\n", LENGTH_POW, CELLS_PER_RECORD, WRITER_BACKOFF );
	}


	@Setup
	public void setup() {
	}

	@TearDown
	public void tearDown() {
		states.clear();
	}

	@Setup( Level.Iteration )
	public void attachThread() {
		//TODO set thread affinity
	}

	public static final List<ThreadState> states = new CopyOnWriteArrayList<ThreadState>();

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
		writeEntry( ts, ts.id, ts.count );
		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	@GenerateMicroBenchmark
	@Group( "writeAndPayloadAndDummyDrain" )
	@GroupThreads( 1 )
	public void dummyDrainer() {
		//mostly we do not care about drain latency here, measure just to be aware of it
		try {
			for( final ThreadState ts : states ) {
				ts.sequencer.drainTo( DRAIN_DUMMY );
			}
			BlackHole.consumeCPU( 10 );
		} catch( Throwable e ) {
			e.printStackTrace();
		}
	}

	/*=============================================================================*/

	/*=============================================================================*/

	private void writeEntry( final ThreadState ts,
	                         final int writerId,
	                         final int cellsCount ) {
		final long position = ts.sequencer.claim( cellsCount );
		if( position < 0 ) {
			System.err.println( "Timeout" );
			return;
		}
		//reserve 0-th cell for header
		final ByteBuffer data = ts.buffer;
//		for( int i = 1; i < cellsCount; i++ ) {
//			data.put( position + i, i );
//		}
//		//write header with SA
//		data.put(
//				position,
//				header(
//						LOG_RECORD,
//						writerId,
//						cellsCount - 1
//				)
//		);
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();
		public final int count = CELLS_PER_RECORD;//but 1 for header

		public SCSDSequencer sequencer;
		public Sequencer.Drainer consumingDrainer;
		public ByteBuffer buffer;

//		public ThreadState() throws Exception {
//			final int length = ( 1 << LENGTH_POW );
//
//			this.sequencer = new SCSDSequencer( length );
//			final File file = new File( "" );
//			final FileChannel channel = new FileOutputStream( file ).getChannel();
//			this.buffer = channel.map(
//					FileChannel.MapMode.READ_WRITE,
//					0,
//					length << 3
//			);
//		}

		@Setup
		public void setup() {
			states.add( this );
		}

		@TearDown
		public void destroy() {
			states.remove( this );
		}
	}

	public static void main( final String[] args ) throws Exception {
//		final PerThreadMappedFileBenchmark benchmark = new PerThreadMappedFileBenchmark();
//
//		benchmark.setup();
//
//		final ThreadState[] states = new ThreadState[3];
//		for( int i = 0; i < states.length; i++ ) {
//			final ThreadState state = new ThreadState();
//			states[i] = state;
//			new Thread() {
//				@Override
//				public void run() {
//					for(; ; ) {
//						benchmark.writer2( state );
//					}
//				}
//			}.start();
//		}
//
//		new Thread() {
//			@Override
//			public void run() {
//				for(; ; ) {
//					benchmark.readingDrainer();
//				}
//			}
//		}.start();
//
//		Thread.sleep( 300000 );
	}

}
