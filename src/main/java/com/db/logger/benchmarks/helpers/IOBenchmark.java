package com.db.logger.benchmarks.helpers;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

/**
 * Benchmark raw write speed for IO and NIO
 * Scenario is to write chunk of data 1024 bytes length on each invocation,
 * and rely on BufferedStream in case of IO, or on manually-implemented ByteBuffer
 * buffering in case of NIO (while buffer size is -Dbuffer-size, default 2^14)
 * <p/>
 * I use single thread and re-create file on each benchmark iteration
 * <p/>
 * "ops" == "bytes", so ops/us -> bytes per microseconds
 *
 * @author cherrus
 *         created 11/23/13 at 2:04 PM
 */
@BenchmarkMode( { Mode.Throughput } )
@OutputTimeUnit( TimeUnit.MICROSECONDS )
@State( Scope.Benchmark )
@Threads( 1 )
public class IOBenchmark {

	public static final int BUFFER_SIZE = Integer.getInteger( "buffer-size", 1 << 14 );
	public static final int BYTES_AT_ONCE = 1024;
	public static final boolean USE_DIRECT_BUFFER = Boolean.getBoolean( "use-direct-buffer" );

	static {
		System.err.println( "buffer=" + BUFFER_SIZE + ", batch=" + BYTES_AT_ONCE + ", useDirect=" + USE_DIRECT_BUFFER );
	}

	/** pattern to write. Only size matters */
	public final byte[] stamp = new byte[BYTES_AT_ONCE];
	public File file;

	public FileOutputStream fileStream;
	public FileChannel fileChannel;

	public ByteBuffer byteBuffer;
	public BufferedOutputStream bufferedStream;


	@Setup( Level.Iteration )
	public void setup() throws Exception {
		file = File.createTempFile( "bench", ".tmp" );
		file.deleteOnExit();

		fileStream = new FileOutputStream( file );
		bufferedStream = new BufferedOutputStream( fileStream, BUFFER_SIZE );
		fileChannel = fileStream.getChannel();

		if( USE_DIRECT_BUFFER ) {
			byteBuffer = ByteBuffer.allocateDirect( BUFFER_SIZE );
		} else {
			byteBuffer = ByteBuffer.allocate( BUFFER_SIZE );
		}
	}

	@TearDown( Level.Iteration )
	public void tearDown() throws Exception {
		if( fileStream != null ) {
			fileStream.close();
		}
		if( fileChannel != null ) {
			fileChannel.close();
		}
		if( file.exists() ) {
			file.delete();
		}
	}

	@GenerateMicroBenchmark
	@Threads( 1 )
	@OperationsPerInvocation( BYTES_AT_ONCE )
	public void inputStreamWriter() throws Exception {
		bufferedStream.write( stamp );
	}

	@GenerateMicroBenchmark
	@Threads( 1 )
	@OperationsPerInvocation( BYTES_AT_ONCE )
	public void channelWriter() throws Exception {
		if( byteBuffer.remaining() < BYTES_AT_ONCE ) {
			byteBuffer.flip();
			fileChannel.write( byteBuffer );
			byteBuffer.clear();
		}
		byteBuffer.put( stamp );
	}

	public static void main( final String[] args ) throws Exception {

		final IOBenchmark benchmark = new IOBenchmark();
		try {
			benchmark.setup();
			for( int i = 0; i < 10000; i++ ) {
				benchmark.inputStreamWriter();
			}
		} finally {
			benchmark.tearDown();
		}
	}
}
