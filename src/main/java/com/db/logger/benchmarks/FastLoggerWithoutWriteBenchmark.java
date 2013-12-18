package com.db.logger.benchmarks;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.db.logger.api.FluentLogBuilder;
import com.db.logger.api.LogMessage;
import com.db.logger.api.impl.logger.*;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.buffer.UnsafeCircularLongsBuffer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

/**
 * Benchmark of FastLoggerImpl with dummy writer: just in-memory byte buffer which
 * is cleared on flush. So, no real IO involved, nut apart from it, all other parts
 * benchmarked
 *
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class FastLoggerWithoutWriteBenchmark {
	private static final Logger log = Logger.getLogger( FastLoggerWithoutWriteBenchmark.class );
	public static final int LENGTH_POW = Integer.getInteger( "length-pow", 14 );
	public static final int LENGTH = 1 << LENGTH_POW;
	/**
	 * It's CELLS-1 arguments + 1 cell for header -- to be consistent with previous
	 * benchmarks
	 */
	public static final int CELLS_PER_RECORD = Integer.getInteger( "cells-per-record", 8 );//8longs = 1 cache line
	public static final int PAYLOAD = Integer.getInteger( "writer-backoff", 20 );

	public static final WaitingStrategy WAITING_STRATEGY = new WaitingStrategy.LimitedSpinning( 1024 * 128 );

	public static final AtomicInteger ID_GENERATOR = new AtomicInteger( 1 );

	static {
		BasicConfigurator.configure();
		System.out.printf( "len=%d, record=%d, payload=%d\n", LENGTH, CELLS_PER_RECORD, PAYLOAD );
	}

	public ICircularLongsBuffer buffer;
	public MCSDSequencer sequencer;

	public FastLoggerImpl logger;

	@Setup
	public void setup() {
		buffer = new UnsafeCircularLongsBuffer( LENGTH, RecordHelper.NOT_SET );
		sequencer = new MCSDSequencer( LENGTH );
		logger = new FastLoggerImpl(
				new ThreadFactory() {
					@Override
					public Thread newThread( final Runnable r ) {
						return new Thread( r );
					}
				},
				buffer,
				WAITING_STRATEGY,
				new FakeRawWriter()
		);
		logger.startDraining();
	}

	@TearDown
	public void tearDown() throws Exception {
		logger.stopDraining();
	}

	@Setup( Level.Iteration )
	public void setupIteration() {
		//TODO set thread affinity
	}

	/*=============================================================================*/
//	@GenerateMicroBenchmark
//	@Group( "payload" )
//	@GroupThreads( 3 )//to be consistent with others
//	public void payloadAlone() {
//		FluentLogBuilder logBuilder = DummyLogBuilder.INSTANCE.with( 5d );
//		final int count = CELLS_PER_RECORD;
//		for( int i = 1; i < count; i++ ) {
//			logBuilder = logBuilder.with( ( long ) i );
//		}
//		logBuilder.log();
//		BlackHole.consumeCPU( PAYLOAD );
//	}

	@GenerateMicroBenchmark
	@Group( "logSimpleAndPayload" )
	@GroupThreads( 3 )
	public void writeSimpleMessage( final ThreadStateSimple ts ) {
		if( ts.simpleMessage == null ) {
			ts.setup( this );
		}
		FluentLogBuilder logBuilder = ts.simpleMessage.with( 5d );
		final int count = CELLS_PER_RECORD - 1;
		for( int i = 1; i < count; i++ ) {
			logBuilder = logBuilder.with( ( long ) i );
		}
		logBuilder.submit();

		BlackHole.consumeCPU( PAYLOAD );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "logTLAndPayload" )
	@GroupThreads( 3 )
	public void writeTLMessage( final ThreadStateTL ts ) {
		if( ts.threadLocalMessage == null ) {
			ts.setup( this );
		}
		FluentLogBuilder logBuilder = ts.threadLocalMessage.with( 5d );
		final int count = CELLS_PER_RECORD - 1;
		for( int i = 1; i < count; i++ ) {
			logBuilder = logBuilder.with( ( long ) i );
		}
		logBuilder.submit();

		BlackHole.consumeCPU( PAYLOAD );
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "logAndPayload" )
	@GroupThreads( 3 )
	public void writeRawMessage( final ThreadState ts ) {
		FluentLogBuilder logBuilder = logger.log( ts.message )
				.with( 5d );
		final int count = CELLS_PER_RECORD - 1;
		for( int i = 1; i < count; i++ ) {
			logBuilder = logBuilder.with( ( long ) i );
		}
		logBuilder.submit();

		BlackHole.consumeCPU( PAYLOAD );
	}

//	@GenerateMicroBenchmark
//	@Group( "lookupMessageInfo" )
//	@GroupThreads( 3 )
//	public Object lookupMessageInfo( final ThreadState ts ) {
//		return logger.lookupMessageInfo( ts.message );
//	}

	@State( Scope.Thread )
	public static class ThreadState {
		public final int id = ID_GENERATOR.incrementAndGet();

		public final String message;

		public ThreadState() {
			final StringBuilder sb = new StringBuilder();
			sb.append( id );
			//actually, it's CELLS-1 arguments + 1 cell for header -- to be consistent
			//with previous benchmarks
			for( int i = 1; i < CELLS_PER_RECORD; i++ ) {
				sb.append( " %d" );
			}
			message = sb.toString();
		}
	}

	@State( Scope.Thread )
	public static class ThreadStateSimple extends ThreadState {

		public LogMessage simpleMessage;

		public void setup( final FastLoggerWithoutWriteBenchmark b ) {
			simpleMessage = b.logger.messageSimple( message );
		}
	}

	@State( Scope.Thread )
	public static class ThreadStateTL extends ThreadState {

		public LogMessage threadLocalMessage;

		public void setup( final FastLoggerWithoutWriteBenchmark b ) {
			threadLocalMessage = b.logger.messageThreadLocal( message );
		}
	}

	public static void main( final String[] args ) throws Exception {
//		final FastLoggerWithoutWriteBenchmark benchmark = new FastLoggerWithoutWriteBenchmark();
//
//		benchmark.setup();
//		final int BATCH = 1000000;
//		final ThreadStateSimple[] states = new ThreadStateSimple[3];
//		for( int i = 0; i < states.length; i++ ) {
//			final ThreadStateSimple state = new ThreadStateSimple();
//			states[i] = state;
//			final Thread thread = new Thread() {
//				@Override
//				public void run() {
//					for( int turn = 0; ; turn += BATCH ) {
//						for( int i = 0; i < BATCH; i++ ) {
//							benchmark.writeFormatted( state );
//						}
//						log.info( "Thread " + state.id + ": " + turn );
//					}
//				}
//			};
//			thread.setDaemon( true );
//			thread.start();
//		}
//
//		Thread.sleep( 30000 );
//		benchmark.tearDown();
	}

}