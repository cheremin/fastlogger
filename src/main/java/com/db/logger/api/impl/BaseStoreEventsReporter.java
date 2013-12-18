package com.db.logger.api.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.db.logger.io.storage.RecordStorage;
import com.db.logger.io.storage.RecordWriter;
import com.db.logger.io.storage.impl.FileRecordStorageBase;
import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author ruslan
 *         created 03.09.13 at 14:16
 */
public class BaseStoreEventsReporter implements Closeable {
	private static final Log log = LogFactory.getLog( BaseStoreEventsReporter.class );

	private final FileRecordStorageBase storage;
	private final int maxRecordsPerFile;



	public BaseStoreEventsReporter( final int maxRecordsPerFile,
	                                final FileRecordStorageBase storage ) {
		checkArgument( storage != null, "storage can't be null" );
		checkArgument( maxRecordsPerFile > 0, "maxRecordsPerFile(%s) must be >0", maxRecordsPerFile );

		this.maxRecordsPerFile = maxRecordsPerFile;
		this.storage = storage;
	}

	protected RecordWriter writer = null;

	protected RecordWriter ensureWriterOpened() {
		if( writer == null ) {
			try {
//				if( !storage.opened() ) {
//					storage.open( CREATE_IF_NOT_EXISTS, BaseMetadata.EMPTY
//							.extend( CAUSE_IDS_COUNT_KEY, String.valueOf( maxCauseIdsCount ) )
//							.extend( TAGS_COUNT_KEY, String.valueOf( maxTagsCount ) )
//							.extend( PAYLOADS_COUNT_KEY, String.valueOf( maxPayloadsCount ) ) );
//				}
				writer = storage.openForAppend( RecordStorage.OpenMode.CREATE_IF_NOT_EXISTS, null );
			} catch( IOException e ) {
				log.error( "Can't open storage " + storage.configuration().file().getAbsolutePath(), e );
			} catch( Throwable t ) {
				log.error( "Can't open storage " + storage.configuration().file().getAbsolutePath(), t );
			}
		}
		return writer;
	}

	protected void afterBatchWritten() {
		//it could be much more then maxRecordsPerFile records stored, if batch is
		// huge enough
		if( storedRecords >= maxRecordsPerFile ) {
			writer.close();
			writer = null;

			try {
				final File oldFile = storage.configuration().file();
				final File newFile = nextFreeFileName( oldFile );
				storage.moveDataTo( newFile );

				log.info( "Dropping off " + oldFile.getAbsolutePath() + " -> " + newFile.getAbsolutePath() );
			} catch( IOException e ) {
				log.error( "Can't move data", e );
			}

			storedRecords = 0;
		}
	}

	protected transient int storedRecords = 0;

//	protected void storeRecord( final Record record ) {
//		try {
//			writer.append( record );
//			storedRecords++;
//		} catch( IOException e ) {
//			log.error( "Can't store record", e );
//			//needs to stop repeating
//			throw Throwables.propagate( e );
//		} catch( Throwable t ) {
//			log.error( "Can't store record", t );
//			//needs to stop repeating
//			throw Throwables.propagate( t );
//		}
//	}

	private transient int fileNumber = 0;

	private File nextFreeFileName( final File file ) {
		final File folder = file.getParentFile();
		final String name = file.getName();
		for(; ; fileNumber++ ) {
			final File newFile = new File( folder, name + '-' + fileNumber );
			if( !newFile.exists() ) {
				return newFile;
			}
		}
	}

	public void close() {
		if( writer != null ) {
			writer.close();
		}
	}
}
