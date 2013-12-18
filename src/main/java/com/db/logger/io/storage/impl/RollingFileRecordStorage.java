package com.db.logger.io.storage.impl;

import com.db.logger.io.storage.RecordReader;
import com.db.logger.io.storage.RecordStorage;
import com.db.logger.io.storage.RecordWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO implement
 *
 * @author cherrus
 *         created 8/16/13 at 8:32 PM
 */
public class RollingFileRecordStorage<T> implements RecordStorage<T> {
    private final FileRecordStorageNIO<T> storage;

    private final int maxRecordsPerFile;

    public RollingFileRecordStorage( final FileRecordStorageNIO<T> storage,
                                     final int maxRecordsPerFile ) {
        checkArgument( storage != null, "storage can't be null" );
        checkArgument( maxRecordsPerFile > 0, "maxRecordsPerFile(%s) must be >0", maxRecordsPerFile );
        this.storage = storage;
        this.maxRecordsPerFile = maxRecordsPerFile;
    }

    @Override
    public Configuration<T> configuration() {
        return storage.configuration();
    }

    @Override
    public Metadata metadata() {
        return null;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public void open( final OpenMode mode,
                      final Metadata metadata ) throws IOException {

    }

    @Override
    public boolean opened() {
        return false;
    }

    @Override
    public long recordsCount() {
        return 0;
    }

    @Override
    public RecordReader<T> openForRead() throws IOException {
        return null;
    }

    @Override
    public RecordWriter<T> openForAppend( final OpenMode mode,
                                          final Metadata metadata ) throws IOException {
        return null;
    }
}
