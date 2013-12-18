package com.db.logger.benchmarks.helpers;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;

/**
 * Here we measure raw sequencer performance: just position claiming/reclaiming
 *
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class VolatileBenchmark {
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 0 );

	static {
		System.err.println( "backoff=" + WRITER_BACKOFF );
	}

	public volatile long counter;

	@Setup
	public void setup() {
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
	@Group( "writeAlone" )
	@GroupThreads( 1 )
	public void writerSolo() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		counter = 3;
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "write4" )
	@GroupThreads( 4 )//actually it's (CORES)
	public void writer4() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		counter = 4;
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "writeRead31" )
	@GroupThreads( 3 )//actually it's (CORES-1)
	public void writer3() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		counter = 5;
	}

	@GenerateMicroBenchmark
	@Group( "writeRead31" )
	@GroupThreads( 1 )
	public long reader1() {
		return counter;
	}

	/*=============================================================================*/

	@GenerateMicroBenchmark
	@Group( "writeRead22" )
	@GroupThreads( 2 )
	public void writer2() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		counter = 6;
	}

	@GenerateMicroBenchmark
	@Group( "writeRead22" )
	@GroupThreads( 2 )
	public long reader2() {
		return counter;
	}
	/*=============================================================================*/

	@GenerateMicroBenchmark
	@Group( "writeRead13" )
	@GroupThreads( 1 )
	public void writer1() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		counter = 7;
	}

	@GenerateMicroBenchmark
	@Group( "writeRead13" )
	@GroupThreads( 3 )
	public long reader3() {
		return counter;
	}
	/*=============================================================================*/


	public static void main( final String[] args ) throws Exception {
		final VolatileBenchmark benchmark = new VolatileBenchmark();

		benchmark.setup();


		Thread.sleep( 300000 );
	}
}
