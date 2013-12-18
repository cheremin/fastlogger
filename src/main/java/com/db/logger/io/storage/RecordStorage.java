package com.db.logger.io.storage;

import java.io.IOException;

/**
 * @author ruslan
 *         created 11.08.13 at 13:33
 */
public interface RecordStorage<T> {

    public Configuration<T> configuration();

    /** @return empty metadata if storage not opened yet */
    public Metadata metadata();

    /**
     * @return is storage actually exists. Can be used without {@linkplain
     *         #open(com.db.logger.io.storage.RecordStorage.OpenMode, com.db.logger.io.storage.RecordStorage.Metadata)}-ed
     *         before
     */
    public boolean exists();

    /**
     * Delete (clear) storage if it exists, do nothing otherwise.
     * Can be used without {@linkplain #open(com.db.logger.io.storage.RecordStorage.OpenMode,
     * com.db.logger.io.storage.RecordStorage.Metadata)}-ed before
     */
    public void remove();

    enum OpenMode {
        CREATE_EMPTY,
        CREATE_IF_NOT_EXISTS,
        LOAD_EXISTENT;
    }

    /**
     * Storage must be opened before actual use (read/write/query metadata).
     *
     * @throws IllegalStateException
     *         if already opened
     */
    public void open( final OpenMode mode,
                      final Metadata metadata ) throws IOException;

    public boolean opened();

    /**
     * @return records count currently in storage. Value may be innacurate if
     *         appending is currently in process 'cos of buffering.
     * @throws IllegalStateException
     *         if storage was not opened before calling the
     *         method
     */
    public long recordsCount();

    /**
     * TODO may be implicitly open(loadExisting)?
     *
     * @throws IllegalStateException
     *         if writer is currently opened
     */
    public RecordReader<T> openForRead() throws IOException;

    /**
     * Only one writer allowed to be opened at time
     *
     * @throws IllegalStateException
     *         if writer was already opened, but not closed
     */
    public RecordWriter<T> openForAppend( final OpenMode mode,
                                          final Metadata metadata ) throws IOException;

    public interface Configuration<T> {
        public ObjectSerializerFactory<T> serializerFactory();
        //to be extended with custom-storage-related data
    }

    /**
     * Set of [key,value] pairs.
     * <p/>
     * Although keys and values exposed as strings, it is expected them to be ASCII
     * only (so, they are actually byte[], exposed as Strings for easy of use)
     */
    public interface Metadata {

        public String[] keys();

        /** @return null, if nothing stored under the key */
        public String value( final String key );

        public Metadata extend( final String key,
                                final String value );
    }
}
