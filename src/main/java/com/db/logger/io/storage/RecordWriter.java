package com.db.logger.io.storage;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * @author cherrus
 *         created 11/14/13 at 10:17 AM
 */
public interface RecordWriter<T> extends Closeable, Flushable/*, AutoCloseable*/ {
    /** @return metadata stored alongside the storage. Can't be modified */
    public RecordStorage.Metadata metadata();

    public void append( final T data ) throws IOException;

    @Override
    public void flush() throws IOException;

    @Override
    public void close();
}
