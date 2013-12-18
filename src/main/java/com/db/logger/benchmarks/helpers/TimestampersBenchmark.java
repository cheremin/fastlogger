package com.db.logger.benchmarks.helpers;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

/**
 * @author ruslan
 *         created 22.11.13 at 22:36
 */
@BenchmarkMode( { Mode.AverageTime, Mode.SampleTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
public class TimestampersBenchmark {

	@GenerateMicroBenchmark
	@OperationsPerInvocation( 100 )
	public long testTimeMillis() {
		long l = 0;
		for( int i = 0; i < 100; i++ ) {
			l += System.currentTimeMillis();
		}
		return l;
	}

	@OperationsPerInvocation( 100 )
	@GenerateMicroBenchmark
	public long testNanoTime() {
		long l = 0;
		for( int i = 0; i < 100; i++ ) {
			l += System.nanoTime();
		}
		return l;
	}

}
