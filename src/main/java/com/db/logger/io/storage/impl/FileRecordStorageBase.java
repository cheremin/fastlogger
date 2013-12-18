package com.db.logger.io.storage.impl;

import com.db.logger.io.storage.*;
import gnu.trove.procedure.TObjectProcedure;
import net.jcip.annotations.NotThreadSafe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * @author cherrus
 *         created 8/14/13 at 4:51 PM
 */
@NotThreadSafe
public abstract class FileRecordStorageBase<T> implements RecordStorage<T>, RecordReader<T> {

    private final FileConfiguration<T> configuration;

    protected File dataFile;
    protected File metadataFile;

    /**
     * Initialized {@linkplain ObjectSerializerFactory#createBy(com.db.logger.io.storage.RecordStorage.Metadata,
     * boolean)}
     * with current {@linkplain #metadata}, if assigned. May be null
     */
    protected ObjectSerializer<T> initializedSerializer = null;
    /** assigned on storage opened/created */
    protected Metadata metadata = null;

    public FileRecordStorageBase( final FileConfiguration<T> configuration ) {
        checkArgument( configuration != null, "configuration can't be null" );

        this.configuration = configuration;

        this.dataFile = configuration.file();
        this.metadataFile = new File( dataFile.getAbsolutePath() + ".meta" );
    }

    @Override
    public FileConfiguration<T> configuration() {
        return configuration;
    }

    public boolean opened() {
        return metadata != null;
    }

    @Override
    public Metadata metadata() {
        if( metadata == null ) {
            return BaseMetadata.EMPTY;
        }
        return metadata;
    }

    @Override
    public boolean exists() {
        return metadataFile.exists();
    }

    @Override
    public void remove() {
        checkState( !writerOpened(),
                "Can't remove storage [%s]: close opened writer first", dataFile.getAbsolutePath() );

        final boolean deletedData = !dataFile.exists() || dataFile.delete();
        checkState( deletedData, "Can't remove %s", dataFile.getAbsolutePath() );

        final boolean deletedMeta = !metadataFile.exists() || metadataFile.delete();
        checkState( deletedMeta, "Can't remove %s", metadataFile.getAbsolutePath() );

        //i.e storage not open()-ed anymore
        this.initializedSerializer = null;
        this.metadata = null;
    }

    /**
     * Move data and metadata to newLocation.
     * <p/>
     * Can be used without
     * {@linkplain #open(com.db.logger.io.storage.RecordStorage.OpenMode,
     * com.db.logger.io.storage.RecordStorage.Metadata)}-ed before
     */
    public void moveTo( final File newLocation ) throws IOException {
        checkState( !writerOpened(),
                "Can't move storage [%s]: close opened writer first", dataFile.getAbsolutePath() );
        final File newMeta = new File( newLocation + ".meta" );

        if( !dataFile.renameTo( newLocation ) ) {
            throw new IOException( "Can't move " + dataFile + " -> " + newLocation );
        }
        if( !metadataFile.renameTo( newMeta ) ) {
            throw new IOException( "Can't move " + metadataFile + " -> " + newMeta );
        }

        dataFile = newLocation;
        metadataFile = newMeta;
    }

    /**
     * Move data and metadata to newLocation, but leave this storage to point to
     * previous location (so leaving this storage empty, i.e. not exists). You should
     * re-create current storage if you suppose to use it father (see {@linkplain
     * #open(com.db.logger.io.storage.RecordStorage.OpenMode, com.db.logger.io.storage.RecordStorage.Metadata)})
     * <p/>
     * Can be used without {@linkplain #open(com.db.logger.io.storage.RecordStorage.OpenMode,
     * com.db.logger.io.storage.RecordStorage.Metadata)}-ed before
     */
    public void moveDataTo( final File newLocation ) throws IOException {
        checkState( !writerOpened(),
                "Can't move storage [%s]: close opened writer first", dataFile.getAbsolutePath() );
        final File newMeta = new File( newLocation + ".meta" );
        if( !dataFile.renameTo( newLocation ) ) {
            throw new IOException( "Can't move " + dataFile + " -> " + newLocation );
        }
        if( !metadataFile.renameTo( newMeta ) ) {
            throw new IOException( "Can't move " + metadataFile + " -> " + newMeta );
        }

        //i.e storage not open()-ed anymore
        this.metadata = null;
        this.initializedSerializer = null;
    }

    @Override
    public long recordsCount() {
        checkState( opened(), "recordsCount available only on opened storage" );
        final int recordSize = initializedSerializer.recordSize();
        return dataFile.length() / recordSize;
    }


    private void create( final Metadata metadata ) throws IOException {
        checkArgument( metadata != null, "metadata can't be null if new storage created" );
        dataFile.getParentFile().mkdirs();
        dataFile.createNewFile();

        final ObjectSerializerFactory<T> serializerFactory = configuration().serializerFactory();
        final ObjectSerializer<T> serializer = serializerFactory.createBy( metadata, true );
        final Metadata metadataEx = serializer.extend( metadata );
        writeMetadata( metadataEx, metadataFile );

        initializeOpenedStorage( serializer, metadataEx );
    }

    private void load() throws IOException {
        this.metadata = readMetadata( metadataFile );
        final ObjectSerializerFactory<T> serializerFactory = configuration().serializerFactory();
        final ObjectSerializer<T> serializer = serializerFactory.createBy( metadata, false );

        initializeOpenedStorage( serializer, metadata );
    }

    private void initializeOpenedStorage( final ObjectSerializer<T> serializer,
                                          final Metadata metadata ) {
        this.metadata = metadata;
        this.initializedSerializer = serializer;
    }

    @Override
    public void open( final OpenMode mode,
                      final Metadata metadata ) throws IOException {
        checkArgument( mode != null, "mode can't be null" );
        checkState( !opened(), "Already opened" );

        switch( mode ) {
            case CREATE_EMPTY: {
                remove();

                create( metadata );
                return;
            }
            case CREATE_IF_NOT_EXISTS: {
                if( exists() ) {
                    load();
                } else {
                    create( metadata );
                }
                return;
            }
            case LOAD_EXISTENT: {
                load();
                return;
            }
            default:
                throw new IllegalStateException( "Unknown mode " + mode );
        }
    }

    @Override
    public RecordReader<T> openForRead() throws IOException {
        if( !opened() ) {
            open( OpenMode.LOAD_EXISTENT, null );
        }
        checkState( opened(),
                "Storage [%s] is not open()-ed", dataFile.getAbsolutePath() );
        checkState( !writerOpened(),
                "Can't read while writer opened, and not closed yet" );
        return this;
    }

    /**
     * There should be <=1 writer at given point of time, so we store it here, and
     * do not allow to create another one while this one is here
     */
    private transient RecordWriter<T> openedWriter = null;

    @Override
    public RecordWriter<T> openForAppend( final OpenMode mode,
                                          final Metadata metadata ) throws IOException {
        if( !opened() ) {
            open( mode, metadata );
        }
        checkState( opened(),
                "Storage [%s] is not open()-ed", dataFile.getAbsolutePath() );
        checkState( !writerOpened(),
                "Writer already opened, and not closed yet -- can't open more then 1 Writer" );

        openedWriter = createWriter();
        return openedWriter;
    }

    public abstract RawWriter openForAppendRaw( final OpenMode mode,
                                                final Metadata metadata ) throws IOException;

    protected boolean writerOpened() {
        return openedWriter != null;
    }

    protected abstract RecordWriter<T> createWriter() throws IOException;

    protected void closeWriter( final RecordWriter<T> writer ) {
        if( openedWriter == writer ) {
            openedWriter = null;
        }
    }

    @Override
    public abstract int fetchRecords( final int startRecord,
                                      final int maxRecordCount,
                                      final TObjectProcedure<? super T> processor,
                                      final T sharedDataHolder ) throws IOException;

    @Override
    public String toString() {
        return String.format(
                "%s[%s]",
                getClass().getSimpleName(),
                dataFile.getAbsolutePath()
        );
    }

    private static RecordStorage.Metadata readMetadata( final File metadataFile ) throws IOException {
        checkState( metadataFile.exists(), "Metadata[%s] not exists", metadataFile.getAbsolutePath() );
        final FileInputStream inputStream = new FileInputStream( metadataFile );
        try {
            final Properties properties = new Properties();
            properties.load( inputStream );
            return new BaseMetadata( ( Map ) properties );
        } finally {
            inputStream.close();
        }
    }

    private static void writeMetadata( final RecordStorage.Metadata metadata,
                                       final File metadataFile ) throws IOException {
        final Properties properties = new Properties();
        for( final String key : metadata.keys() ) {
            final String value = metadata.value( key );
            properties.put( key, value );
        }
        final FileOutputStream outputStream = new FileOutputStream( metadataFile );
        try {
            properties.store( outputStream, "" );
        } finally {
            outputStream.close();
        }
    }
}


