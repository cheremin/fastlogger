package com.db.logger.benchmarks.helpers;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

/**
 *
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Benchmark )
public class ThreadLocalBenchmark {
//	public static final int PAYLOAD = Integer.getInteger( "writer-backoff", 0 );

	public ThreadLocal<Long> counter = new ThreadLocal<Long>(){
		@Override
		protected Long initialValue() {
			return new Long( 1025 );
		}
	};

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
	public long threadLocalGet() {
		return counter.get();
	}

	public static void main( final String[] args ) throws Exception {
		final ThreadLocalBenchmark benchmark = new ThreadLocalBenchmark();

		benchmark.setup();


		Thread.sleep( 300000 );
	}
}
