package com.db.logger.api.impl.logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;

import com.db.logger.api.FastLogger;
import com.db.logger.api.FluentLogBuilder;
import com.db.logger.api.LogMessage;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import com.db.logger.api.impl.logger.formatters.RawLogMessage;
import com.db.logger.api.impl.logger.formatters.SimpleLogMessage;
import com.db.logger.io.storage.RawWriter;
import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.db.logger.api.impl.logger.RecordHelper.*;
import static com.db.logger.api.impl.logger.RecordHelper.isValidHeader;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author ruslan
 *         created 24.08.13 at 15:14
 */
public class FastLoggerImpl implements FastLogger {
	private static final Log log = LogFactory.getLog( FastLoggerImpl.class );

	private final ThreadFactory threadFactory;

	private final RingBuffer ringBuffer;

	private final RawWriter writer;


	public FastLoggerImpl( final ThreadFactory threadFactory,
	                       final ICircularLongsBuffer buffer,
	                       final WaitingStrategy waitingStrategy,
	                       final RawWriter writer ) {
		checkArgument( threadFactory != null, "threadFactory can't be null" );
		checkArgument( waitingStrategy != null, "waitingStrategy can't be null" );
		checkArgument( buffer != null, "buffer can't be null" );
		checkArgument( writer != null, "writer can't be null" );


		this.threadFactory = threadFactory;

		final int length = buffer.length();
		this.ringBuffer = new RingBuffer(
				new MCSDSequencer( length ),
				buffer,
				waitingStrategy
		);

		this.writer = writer;
	}

//	private final ThreadLocal<R> localEntryBuilder = new ThreadLocal<R>() {
//		@Override
//		protected R initialValue() {
//			return builderFactory.apply( timestampSource );
//		}
//	};

	@Override
	public synchronized SimpleLogMessage messageSimple( final String messageFormat ) {
		final MessageInfo messageInfo = lookupMessageInfo( messageFormat );
		return new SimpleLogMessage(
				messageFormat,
				messageInfo.formatId,
				messageInfo.argumentsCount,
				ringBuffer
		);
	}

	private final ThreadLocal<RawLogMessage> holder = new ThreadLocal<RawLogMessage>() {
		@Override
		protected RawLogMessage initialValue() {
			return new RawLogMessage( ringBuffer );
		}
	};


	@Override
	public synchronized LogMessage messageThreadLocal( final String messageFormat ) {
		final MessageInfo messageInfo = lookupMessageInfo( messageFormat );

		return new ThreadLocalLogMessage( messageInfo );
	}

	private final MessagesCatalog messages = new MessagesCatalog( 2048 );

	public MessageInfo lookupMessageInfo( final String messageFormat ) {
		return messages.lookupMessageInfo( messageFormat );
	}

	public LogMessage log( final String messageFormat ) {
		final MessageInfo messageInfo = lookupMessageInfo( messageFormat );
		final RawLogMessage message = holder.get();
		return message.setup( messageInfo ).start();
	}

	private Thread drainerThread = null;

	public synchronized void startDraining() {
		if( drainerThread == null ) {
			final Drainer drainer = new Drainer(
					ringBuffer, new ConsumingDrainer(
					ringBuffer.buffer(),
					writer
			)
			);
			drainerThread = threadFactory.newThread(
					drainer
			);
			drainerThread.start();
		} else {
			log.error( "ignore repeated startDraining() call" );
		}
	}

	public synchronized void stopDraining() throws InterruptedException {
		if( drainerThread != null ) {
			drainerThread.interrupt();
			drainerThread.join();
			drainerThread = null;
		}
	}

	//	public interface Reporter extends Closeable {
//
//		public void process( final ICircularLongArray buffer,
//		                     final long firstSequence,
//		                     final long sentinelSequence );
//
//		public void close();
//	}

	private static class Drainer implements Runnable, MCSDSequencer.Drainer {
		//TODO setup!
		private static final int MIN_PERIOD_MS = 1;
		private static final int MAX_PERIOD_MS = 1000;

		private long reportingPeriodMs = MIN_PERIOD_MS;

		private final ConsumingDrainer consumer;
		private final RingBuffer ringBuffer;

		private Drainer( final RingBuffer ringBuffer,
		                 final ConsumingDrainer consumer ) {
			this.consumer = consumer;
			this.ringBuffer = ringBuffer;
		}

		@Override
		public void run() {
			while( !Thread.interrupted() ) {
				try {
					ringBuffer.drainTo( this );
					Thread.yield();
//					reportingPeriodMs = nextWaitPeriod(
//							processedRecords,
//							ringBuffer.length(),
//							reportingPeriodMs
//					);
//					Thread.sleep( reportingPeriodMs );
//				} catch( InterruptedException e ) {
//					log.info( "Reporting engine " + consumer + " interrupted" );
//					return;
				} catch( Throwable t ) {
					log.error( "Reporting engine " + consumer + " error", t );
				}
			}
		}

		private int processedRecords = -1;

		@Override
		public int available( final long startSequence,
		                      final long sentinelSequence ) {
			processedRecords = consumer.available(
					startSequence,
					sentinelSequence
			);
			return processedRecords;
		}

		private long nextWaitPeriod( final int processed,
		                             final int maxProcessed,
		                             final long currentWaitMs ) {
			//TODO smooth transition
			//TODO limit max/min wait time
			if( processed < maxProcessed / 8 ) {
				final long wait = currentWaitMs * 2;
				return Math.min( MAX_PERIOD_MS, wait );
			} else if( processed > maxProcessed / 2 ) {
				final long wait = currentWaitMs / 2;
				return Math.max( MIN_PERIOD_MS, wait );
			} else {
				return currentWaitMs;
			}
		}
	}

	private static class ConsumingDrainer implements Sequencer.Drainer {
		private static final int SPINS_PER_TURN = 256;

		private final ICircularLongsBuffer buffer;
		private final RawWriter writer;

		private ConsumingDrainer( final ICircularLongsBuffer buffer,
		                          final RawWriter writer ) {
			this.buffer = buffer;
			this.writer = writer;
		}

		private int spinsAvailable;

		@Override
		public int available( final long startSequence,
		                      final long sentinelSequence ) {
			spinsAvailable = SPINS_PER_TURN;
			ByteBuffer output = writer.buffer();
			try {
				for( long pos = startSequence; pos < sentinelSequence; pos++ ) {
					final long header = readHeader( pos );
					if( !isValidHeader( header ) ) {
						return ( int ) ( pos - startSequence );
					}
					final RecordType type = type( header );
					final int formatId = formatId( header );
					final int argumentsCount = argumentsCount( header );

					//ensure output has space
					if( output.remaining() < ( argumentsCount + 1 ) * 8 ) {
						writer.flush();
						output = writer.buffer();
					}

					//write header + argCount arguments
					for( int i = 0; i <= argumentsCount; i++ ) {
						final long arg = buffer.get( pos + i );
						buffer.put( pos + i, NOT_SET );//need to reclaim each cell!
						output.putLong( arg );
					}

					pos += argumentsCount;
				}
				return ( int ) ( sentinelSequence - startSequence );
			} catch( IOException e ) {
				throw Throwables.propagate( e );
			}
		}

		private long readHeader( final long pos ) {
			for(; spinsAvailable >= 0; spinsAvailable-- ) {
				final long header = buffer.getVolatile( pos );
				if( isValidHeader( header ) ) {
					return header;
				}
			}
			return NOT_SET;
		}
	}


	private final class ThreadLocalLogMessage implements LogMessage {

		private final MessageInfo messageInfo;

		private ThreadLocalLogMessage( final MessageInfo messageInfo ) {
			this.messageInfo = messageInfo;
		}

		@Override
		public FluentLogBuilder with( final double value ) {
			return setupLocal().with( value );
		}

		@Override
		public FluentLogBuilder with( final long value ) {
			return setupLocal().with( value );
		}

		@Override
		public void submit() {
			setupLocal().submit();
		}

		private RawLogMessage setupLocal() {
			final RawLogMessage formatter = holder.get();
			return formatter.setup( messageInfo ).start();
		}

		@Override
		public String format() {
			return messageInfo.format;
		}

		@Override
		public int argumentsCount() {
			return messageInfo.argumentsCount;
		}
	}
}
