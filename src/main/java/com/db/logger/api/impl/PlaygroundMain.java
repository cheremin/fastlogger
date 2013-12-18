package com.db.logger.api.impl;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

import com.db.logger.api.impl.logger.*;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.buffer.UnsafeCircularLongsBuffer;
import com.db.logger.io.storage.RawWriter;
import com.db.logger.timesource.impl.JDKCombinedTimestampSource;
import com.db.logger.timesource.impl.SynchronousJDKOffsetCalculator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;

/**
 * @author cherrus
 *         created 8/12/13 at 10:57 AM
 */
public class PlaygroundMain {
	private static final Log log = LogFactory.getLog( PlaygroundMain.class );

	static {
		BasicConfigurator.configure();
	}

	private static final JDKCombinedTimestampSource TIMESTAMP_SOURCE = new JDKCombinedTimestampSource( new SynchronousJDKOffsetCalculator() );

	public static void main( final String[] args ) throws Exception {

		final int length = ( 1 << 14 );
		final ICircularLongsBuffer buffer = new UnsafeCircularLongsBuffer( length );

		final File file = new File( "output.log" );
		log.info( "File " + file );
		file.delete();
//		file.deleteOnExit();
		final FastLoggerImpl logger = new FastLoggerImpl(
				new ThreadFactory() {
					@Override
					public Thread newThread( final Runnable r ) {
						return new Thread( r );
					}
				},
				buffer,
				WaitingStrategy.SPINNING,
				new RawFileWriter( file )
		);

		logger.startDraining();

		final int workers = Runtime.getRuntime().availableProcessors() - 1;
		final ExecutorService workersPool = Executors.newFixedThreadPool( workers );

		for( int i = 0; i < workers; i++ ) {
			workersPool.submit(
					new Runnable() {
						@Override
						public void run() {
							try {
								for( int i = 0; i < 100000000; i++ ) {
									logger.log( "Message %f -- %d " )
											.with( 25.98 + i )
											.with( 100 - i ).submit();
								}
							} catch( Exception e ) {
								log.error( "Error", e );
							}
						}
					}
			);
		}


		workersPool.shutdown();
		workersPool.awaitTermination( 1000, TimeUnit.SECONDS );
		log.info( "Finished" );
		System.exit( 1 );
	}

	private static class RawFileWriter implements RawWriter {
		private final ByteBuffer buffer;
		private final FileChannel channel;

		public RawFileWriter( final File file ) throws IOException {
			this( new FileOutputStream( file ).getChannel() );
		}

		public RawFileWriter( final FileChannel channel ) {
			this.channel = channel;
			buffer = ByteBuffer.allocateDirect( 1 << 16 );
		}

		@Override
		public ByteBuffer buffer() {
			return buffer;
		}

		@Override
		public void flush() throws IOException {
			buffer.flip();
			channel.write( buffer );
			buffer.clear();
		}

		@Override
		public void close() throws IOException {
			flush();
			channel.close();
		}
	}
}
