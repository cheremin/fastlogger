package com.db.logger.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.db.logger.api.FluentLogBuilder;
import com.db.logger.api.impl.logger.*;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.buffer.UnsafeCircularLongsBuffer;
import com.db.logger.api.impl.logger.formatters.SimpleLogMessageExpanded;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

/**
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class SimpleLogMessageWriteAndDrainBenchmark {
	public static final int LENGTH = Integer.getInteger( "length", 1 << 14 );
	public static final int CELLS_PER_RECORD = Integer.getInteger( "cells-per-record", 8 );//8longs = 1 cache line
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 20 );

	public static final WaitingStrategy WAITING_STRATEGY = new WaitingStrategy.LimitedSpinning( 1024 * 128 );

	public static final AtomicInteger ID_GENERATOR = new AtomicInteger( 1 );

	static {
		System.out.printf( "len=%d, record=%d, payload=%d\n", LENGTH, CELLS_PER_RECORD, WRITER_BACKOFF );
	}

	public ICircularLongsBuffer buffer;
	public MCSDSequencer sequencer;

	public MCSDSequencer.Drainer readingConsumer;

	@Setup
	public void setup() {
		buffer = new UnsafeCircularLongsBuffer( LENGTH, RecordHelper.NOT_SET );
		sequencer = new MCSDSequencer( LENGTH );

		readingConsumer = new ConsumingDrainer( buffer );
	}

	@TearDown
	public void tearDown() {

	}

	@Setup( Level.Iteration )
	public void setupIteration() {
		//TODO set thread affinity
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "payload" )
	public void backoffAlone() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "formatAndReadAndPayload" )
	@GroupThreads( 3 ) //actually it's (CORES-1)
	public void writeFormatted( final ThreadState ts ) {
		if( ts.formatter == null ) {
			ts.setup( this );
		}
		final int count = ts.formatter.argumentsCount();
		FluentLogBuilder logBuilder = ts.formatter.with( 5d );
		for( int i = 1; i < count; i++ ) {
			logBuilder = logBuilder.with( ( long ) i );
		}
		logBuilder.submit();

		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	@GenerateMicroBenchmark
	@Group( "formatAndReadAndPayload" )
	@GroupThreads( 1 )
	public void readingDrainer() {
		try {
			sequencer.drainTo( readingConsumer );
//			BlackHole.consumeCPU( 20 );
		} catch( Throwable e ) {
			e.printStackTrace();
		}
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();

		public SimpleLogMessageExpanded formatter;

		public void setup( final SimpleLogMessageWriteAndDrainBenchmark b ) {
			formatter = new SimpleLogMessageExpanded(
					"",
					id,
					CELLS_PER_RECORD - 1,
					new RingBuffer( b.sequencer, b.buffer, WAITING_STRATEGY )
			);
		}
	}

	public static void main( final String[] args ) throws Exception {
		final SimpleLogMessageWriteAndDrainBenchmark benchmark = new SimpleLogMessageWriteAndDrainBenchmark();

		benchmark.setup();

		final ThreadState[] states = new ThreadState[3];
		for( int i = 0; i < states.length; i++ ) {
			final ThreadState state = new ThreadState();
			states[i] = state;
			new Thread() {
				@Override
				public void run() {
					for(; ; ) {
						benchmark.writeFormatted( state );
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