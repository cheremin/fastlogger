package com.db.logger.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.core.async.AsyncLogger;
import org.apache.logging.log4j.core.async.AsyncLoggerContext;
import org.apache.logging.log4j.core.helpers.ClockFactory;
import org.apache.logging.log4j.core.helpers.CoarseCachedClock;
import org.apache.logging.log4j.message.StringFormatterMessageFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.logic.BlackHole;

/**
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class Log4j20Benchmark {
	public static final int LENGTH = Integer.getInteger( "length", 1 << 14 );
	public static final int CELLS_PER_RECORD = Integer.getInteger( "cells-per-record", 8 );//8longs = 1 cache line
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 20 );

	public static final AtomicInteger ID_GENERATOR = new AtomicInteger( 1 );


	static {
		System.setProperty(
				ClockFactory.PROPERTY_NAME,
				CoarseCachedClock.class.getName()
		);
		System.out.printf( "len=%d, record=%d, backoff=%d\n", LENGTH, CELLS_PER_RECORD, WRITER_BACKOFF );
	}


	public AsyncLoggerContext loggerContext;


	@Setup
	public void setup() {
		loggerContext = new AsyncLoggerContext( "benchmark" );


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
	@Group( "baseline" )
	public void backoffAlone() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "formatAndRead" )
	@GroupThreads( 3 ) //actually it's (CORES-1)
	public void writeFormatted( final ThreadState ts ) {
		if( ts.logger == null ) {
			ts.setup( this );
		}
		final AsyncLogger logger = ts.logger;
		final Object[] arguments = new Object[CELLS_PER_RECORD - 1];
		logger.info( "", arguments );
//		final int count = logMessage.argumentsCount();
//		for( int i = 0; i < count; i++ ) {
//			logMessage = logMessage.with( ( long ) i );
//		}
//		logMessage.submit();

		BlackHole.consumeCPU( WRITER_BACKOFF );
	}

	@GenerateMicroBenchmark
	@Group( "formatAndRead" )
	@GroupThreads( 1 )
	public void readingDrainer() {
		try {
//			sequencer.drainTo( readingConsumer );
//			BlackHole.consumeCPU( 100 );
		} catch( Throwable e ) {
			e.printStackTrace();
		}
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();

		public AsyncLogger logger;

		public void setup( final Log4j20Benchmark b ) {
			logger = new AsyncLogger(
					b.loggerContext,
					"",
					StringFormatterMessageFactory.INSTANCE
			);
		}
	}

	public static void main( final String[] args ) throws Exception {
		final Log4j20Benchmark benchmark = new Log4j20Benchmark();

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