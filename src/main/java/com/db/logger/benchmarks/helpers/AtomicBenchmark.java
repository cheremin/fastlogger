package com.db.logger.benchmarks.helpers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

/**
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class AtomicBenchmark {
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 0 );

	static {
		System.err.println( "payload=" + WRITER_BACKOFF );
	}

	public AtomicLong counter;

	@Setup
	public void setup() {
		counter = new AtomicLong( 0 );
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
	@Group( "writeAlone" )
	@GroupThreads( 4 )
	public long writerSolo(final ThreadState ts) {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return ts.counter.incrementAndGet();
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "write4AndPayload" )
	@GroupThreads( 4 )//actually it's (CORES)
	public long writer4() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return counter.incrementAndGet();
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "write3Read1AndPayload" )
	@GroupThreads( 3 )//actually it's (CORES-1)
	public long writer3() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return counter.incrementAndGet();
	}

	@GenerateMicroBenchmark
	@Group( "write3Read1AndPayload" )
	@GroupThreads( 1 )
	public long reader1() {
		return counter.get();
	}

	/*=============================================================================*/

	@GenerateMicroBenchmark
	@Group( "write2Read2AndPayload" )
	@GroupThreads( 2 )
	public long writer2() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return counter.incrementAndGet();
	}

	@GenerateMicroBenchmark
	@Group( "write2Read2AndPayload" )
	@GroupThreads( 2 )
	public long reader2() {
		return counter.get();
	}
	/*=============================================================================*/

	@GenerateMicroBenchmark
	@Group( "write1Read3AndPayload" )
	@GroupThreads( 1 )
	public long writer1() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return counter.incrementAndGet();
	}

	@GenerateMicroBenchmark
	@Group( "write1Read3AndPayload" )
	@GroupThreads( 3 )
	public long reader3() {
		return counter.get();
	}

	@State( Scope.Thread )
	public static class ThreadState {
		public volatile AtomicLong counter = new AtomicLong();

	}
	/*=============================================================================*/


	public static void main( final String[] args ) throws Exception {
		final AtomicBenchmark benchmark = new AtomicBenchmark();

		benchmark.setup();


		Thread.sleep( 300000 );
	}
}
