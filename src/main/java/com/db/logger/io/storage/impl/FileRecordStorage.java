package com.db.logger.io.storage.impl;

import com.db.logger.io.storage.ObjectSerializer;
import com.db.logger.io.storage.RawWriter;
import com.db.logger.io.storage.RecordWriter;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import gnu.trove.procedure.TObjectProcedure;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author cherrus
 *         created 8/12/13 at 11:29 AM
 */
public class FileRecordStorage<T> extends FileRecordStorageBase<T> {
    private static final Log log = LogFactory.getLog( FileRecordStorage.class );

    public FileRecordStorage( final FileConfiguration<T> configuration ) {
        super( configuration );
    }

    @Override
    public int fetchRecords( final int startRecord,
                             final int maxRecordCount,
                             final TObjectProcedure<? super T> processor,
                             final T sharedDataHolder ) throws IOException {
        checkState( opened(), "Storage must be opened first" );
        checkState( !writerOpened(), "Can't read while writer not closed" );

        final ObjectSerializer<T> serializer = initializedSerializer;
        final int recordSize = serializer.recordSize();

        final long skipBytes = startRecord * recordSize;
        final FileInputStream fis = new FileInputStream( dataFile );
        try {
            fis.skip( skipBytes );
            final long length = dataFile.length();
            final int recordsAvailable = Ints.checkedCast( ( length - skipBytes ) / recordSize );
            final int recordsToRead = Math.min( recordsAvailable, maxRecordCount );
            final int bufferSize = configuration().readBufferSize() * recordSize;
            final BufferedInputStream bis = new BufferedInputStream( fis, bufferSize );
            try {
                final DataInputStream dis = new DataInputStream( bis );
                try {
                    for( int recordsRead = 0; recordsRead < recordsToRead; recordsRead++ ) {
                        //TODO RC minor: check is EOF before calling serializer?
                        final T record = serializer.read(
                                dis,
                                sharedDataHolder
                        );
                        if( !processor.execute( record ) ) {
                            return recordsRead + 1;
                        }
                    }
                } finally {
                    dis.close();
                }
            } finally {
                bis.close();
            }
            return recordsToRead;
        } finally {
            fis.close();
        }
    }

    @Override
    protected RecordWriter<T> createWriter() throws IOException {
        //storage already checked to be opened here
        final ObjectSerializer<T> serializer = initializedSerializer;
        return new writer(
                dataFile,
                serializer
        );
    }

    @Override
    public RawWriter openForAppendRaw( final OpenMode mode,
                                       final Metadata metadata ) throws IOException {
        throw new UnsupportedOperationException( "Method not implemented" );
    }

    private class writer implements RecordWriter<T> {
        private final ObjectSerializer<T> serializer;

        private FileOutputStream outputStream;
        private BufferedOutputStream bufferedStream;
        private DataOutputStream dataStream;

        private writer( final File dataFile,
                        final ObjectSerializer<T> serializer ) throws IOException {
            this.serializer = serializer;

            final int recordSize = serializer.recordSize();

            outputStream = new FileOutputStream( dataFile, true );
            final int writeBufferSize = configuration().writeBufferSize();
            bufferedStream = new BufferedOutputStream(
                    outputStream,
                    recordSize * writeBufferSize
            );
            dataStream = new DataOutputStream( bufferedStream );
        }

        @Override
        public Metadata metadata() {
            return metadata;
        }

        @Override
        public void append( final T data ) throws IOException {
            serializer.write( data, dataStream );
        }

        @Override
        public void flush() throws IOException {
            dataStream.flush();
        }

        @Override
        public void close() {
            Closeables.closeQuietly( dataStream );
            Closeables.closeQuietly( bufferedStream );
            Closeables.closeQuietly( outputStream );
            closeWriter( this );
        }
    }
}
