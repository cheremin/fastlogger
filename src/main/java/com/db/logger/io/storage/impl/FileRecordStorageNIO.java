package com.db.logger.io.storage.impl;

import com.db.logger.io.storage.ObjectSerializerNIO;
import com.db.logger.io.storage.RawWriter;
import com.db.logger.io.storage.RecordWriter;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import gnu.trove.procedure.TObjectProcedure;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

/**
 * TODO RC: ensure only one(?) fetching in progress
 * TODO RC: use file locking to prevent concurrent access to file
 * while storage in progress?
 * <p/>
 *
 * @author cherrus
 *         created 8/12/13 at 11:29 AM
 */
@NotThreadSafe
public class FileRecordStorageNIO<T> extends FileRecordStorageBase<T> {
    private static final Log log = LogFactory.getLog( FileRecordStorageNIO.class );
    /** allocate direct, or heap-based ByteBuffer for read/write operations */
    private final boolean useDirectBuffer;

    public FileRecordStorageNIO( final FileConfiguration<T> configuration ) {
        this( configuration, false );
    }

    public FileRecordStorageNIO( final FileConfiguration<T> configuration,
                                 final boolean useDirectBuffer ) {
        super( configuration );
        this.useDirectBuffer = useDirectBuffer;
    }

    @Override
    public int fetchRecords( final int startRecord,
                             final int maxRecordCount,
                             final TObjectProcedure<? super T> processor,
                             final T sharedDataHolder ) throws IOException {
        checkState( opened(), "Storage must be opened first" );
        checkState( !writerOpened(), "Can't read while writer not closed" );


        final ObjectSerializerNIO<T> serializer = ( ObjectSerializerNIO<T> ) initializedSerializer;
        final int recordSize = serializer.recordSize();
        final int readBufferSize = configuration().readBufferSize();

        final long skipBytes = startRecord * recordSize;
        final RandomAccessFile raf = new RandomAccessFile( dataFile, "r" );
        try {
            final FileChannel channel = raf.getChannel();
            try {
                channel.position( skipBytes );
                final long length = channel.size();
                final int recordsAvailable = Ints.checkedCast( ( length - skipBytes ) / recordSize );
                final int recordsToRead = Math.min( recordsAvailable, maxRecordCount );
                final ByteBuffer buffer = acquireReadBuffer( recordSize * readBufferSize );
                try {
                    buffer.position( buffer.capacity() );//to force initial load
                    for( int recordsRead = 0; recordsRead < recordsToRead; recordsRead++ ) {
                        if( buffer.capacity() - buffer.position() < recordSize ) {
                            buffer.clear();
                            final int bytesRead = channel.read( buffer );
                            if( bytesRead == 0 ) {
                                return recordsRead;
                            }
                            //TODO RC: read all bytes up to broken, and only after throw exception
                            //(i.e. do the best to recover as much as it possible)
                            checkState( bytesRead % recordSize == 0,
                                    "partial record: chunk of %s b while record is %s b", bytesRead, recordSize );
                            buffer.flip();
                        }
                        //allow to read only recordSize bytes
                        buffer.limit( buffer.position() + recordSize );
                        final T record = serializer.read(
                                buffer,
                                sharedDataHolder
                        );
                        checkState( buffer.remaining() == 0,
                                "serializer(%s) not read full record (% remaining)", buffer.remaining() );
                        if( !processor.execute( record ) ) {
                            return recordsRead + 1;
                        }
                    }
                    return recordsToRead;
                } finally {
                    releaseReadBuffer( buffer );
                }
            } finally {
                channel.close();
            }
        } finally {
            raf.close();
        }
    }

    private transient ByteBuffer writeBuffer = null;

    private ByteBuffer writeBuffer( final int expectedSizeBytes ) {
        if( writeBuffer == null
                || writeBuffer.capacity() < expectedSizeBytes ) {
            writeBuffer = allocateByteBuffer( expectedSizeBytes );
        }
        return writeBuffer;
    }

    private transient ByteBuffer readBuffer = null;

    private boolean readBufferAcquired = false;

    /**
     * First buffer will be cached, next request will lead to allocation of fresh
     * new buffers each time
     */
    private ByteBuffer acquireReadBuffer( final int expectedSizeBytes ) {
        if( !readBufferAcquired ) {
            if( readBuffer == null
                    || readBuffer.capacity() < expectedSizeBytes ) {
                readBuffer = allocateByteBuffer( expectedSizeBytes );
            }
            readBufferAcquired = true;
            readBuffer.clear();
            return readBuffer;
        } else {
            log.warn( "Additional read buffer allocated: >1 read requests at time." );
            //always allocate non-direct, since direct buffers free-ed only on full
            // GC, and their extensive allocation is not a good idea
            return ByteBuffer.allocate( expectedSizeBytes );
        }
    }

    private void releaseReadBuffer( final ByteBuffer readBuffer ) {
        if( readBufferAcquired
                && this.readBuffer == readBuffer ) {
            this.readBufferAcquired = false;
        }
    }

    private ByteBuffer allocateByteBuffer( final int expectedSizeBytes ) {
        if( useDirectBuffer ) {
            return ByteBuffer.allocateDirect( expectedSizeBytes );
        } else {
            return ByteBuffer.allocate( expectedSizeBytes );
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s[%s][direct:%b]",
                getClass().getSimpleName(),
                dataFile.getAbsolutePath(),
                useDirectBuffer
        );
    }


    @Override
    public RawWriter openForAppendRaw( final OpenMode mode,
                                       final Metadata metadata ) throws IOException {
        return ( RawWriter ) openForAppend( mode, metadata );
    }

    @Override
    protected writer createWriter() throws IOException {
        //storage already checked to be opened here
        final ObjectSerializerNIO<T> serializer = ( ObjectSerializerNIO<T> ) initializedSerializer;
        return new writer(
                dataFile,
                serializer
        );
    }

    private class writer implements RecordWriter<T>, RawWriter {
        private final ObjectSerializerNIO<T> serializer;

        private FileOutputStream outputStream;
        private FileChannel channel;

        private final ByteBuffer buffer;
        private int recordSize;

        private writer( final File dataFile,
                        final ObjectSerializerNIO<T> serializer ) throws IOException {
            this.serializer = serializer;

            recordSize = serializer.recordSize();
            final int writeBufferSize = configuration().writeBufferSize();
            buffer = writeBuffer( writeBufferSize * recordSize );

            outputStream = new FileOutputStream( dataFile, true );
            channel = outputStream.getChannel();
        }

        @Override
        public Metadata metadata() {
            return metadata;
        }

        @Override
        public ByteBuffer buffer() {
            return buffer;
        }

        @Override
        public void append( final T data ) throws IOException {
            //do not allow serializer to betray us
            buffer.limit( buffer.position() + recordSize );
            serializer.write( data, buffer );
            checkState( buffer.remaining() == 0, "serializer(%s) does not fill %s bytes", serializer, buffer.remaining() );
            if( buffer.capacity() - buffer.limit() < recordSize ) {
                flush();
            }
        }

        @Override
        public void flush() throws IOException {
            if( buffer.position() > 0 ) {
                buffer.flip();
                channel.write( buffer );
                buffer.clear();
            }
        }

        @Override
        public void close() {
            try {
                flush();
            } catch( IOException e ) {
                log.error( "Error in flush " + dataFile.getAbsolutePath(), e );
            } finally {
                Closeables.closeQuietly( channel );
                Closeables.closeQuietly( outputStream );
                closeWriter( this );
            }
        }
    }
}