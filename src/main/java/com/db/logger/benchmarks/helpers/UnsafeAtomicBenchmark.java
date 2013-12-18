package com.db.logger.benchmarks.helpers;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.db.logger.api.impl.logger.UnsafeHelper;
import com.google.common.base.Throwables;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.BlackHole;
import sun.misc.Unsafe;

/**
 * counter is class field, so subject of padding by JMH.
 * <p/>
 * Also, no checks
 *
 * @author ruslan
 *         created 22.11.13 at 20:04
 */
@BenchmarkMode( { Mode.AverageTime } )
@OutputTimeUnit( TimeUnit.NANOSECONDS )
@State( Scope.Group )
public class UnsafeAtomicBenchmark {
	public static final int WRITER_BACKOFF = Integer.getInteger( "writer-backoff", 0 );

	public static final Unsafe UNSAFE = UnsafeHelper.unsafe();

	public static final long counterOffset;

	static {
		try {
			final Field counterField = UnsafeAtomicBenchmark.class.getField( "counter" );
			counterOffset = UNSAFE.objectFieldOffset( counterField );
			System.err.println( "payload=" + WRITER_BACKOFF );
		} catch( NoSuchFieldException e ) {
			throw Throwables.propagate( e );
		}
	}

	public volatile long counter;

	@Setup
	public void setup() {
		counter = 0;
	}

	@TearDown
	public void tearDown() {

	}

	@Setup( Level.Iteration )
	public void attachThread() {
		//TODO set thread affinity
	}

	public long incrementAndGet() {
		for(; ; ) {
			long current = counter;
			long next = current + 1;
			if( UNSAFE.compareAndSwapLong( this, counterOffset, current, next ) ) {
				return next;
			}
		}
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
	public long writerSolo( final ThreadState ts ) {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return ts.counter.incrementAndGet();
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "write4AndPayload" )
	@GroupThreads( 4 )//actually it's (CORES)
	public long writer4() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return incrementAndGet();
	}

	/*=============================================================================*/
	@GenerateMicroBenchmark
	@Group( "write3Read1AndPayload" )
	@GroupThreads( 3 )//actually it's (CORES-1)
	public long writer3() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return incrementAndGet();
	}

	@GenerateMicroBenchmark
	@Group( "write3Read1AndPayload" )
	@GroupThreads( 1 )
	public long reader1() {
		BlackHole.consumeCPU( WRITER_BACKOFF / 3 );
		return counter;
	}

	/*=============================================================================*/

	@GenerateMicroBenchmark
	@Group( "write2Read2AndPayload" )
	@GroupThreads( 2 )
	public long writer2() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return incrementAndGet();
	}

	@GenerateMicroBenchmark
	@Group( "write2Read2AndPayload" )
	@GroupThreads( 2 )
	public long reader2() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return counter;
	}
	/*=============================================================================*/

	@GenerateMicroBenchmark
	@Group( "write1Read3AndPayload" )
	@GroupThreads( 1 )
	public long writer1() {
		BlackHole.consumeCPU( WRITER_BACKOFF );
		return incrementAndGet();
	}

	@GenerateMicroBenchmark
	@Group( "write1Read3AndPayload" )
	@GroupThreads( 3 )
	public long reader3() {
		BlackHole.consumeCPU( WRITER_BACKOFF * 3 );
		return counter;
	}
	/*=============================================================================*/

	@State( Scope.Thread )
	public static class ThreadState {
		public final AtomicLong counter = new AtomicLong();
	}


	public static void main( final String[] args ) throws Exception {
		final UnsafeAtomicBenchmark benchmark = new UnsafeAtomicBenchmark();

		benchmark.setup();


		Thread.sleep( 300000 );
	}
}
