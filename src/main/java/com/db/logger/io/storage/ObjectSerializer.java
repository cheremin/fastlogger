package com.db.logger.io.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ruslan
 *         created 11.08.13 at 14:16
 */
public interface ObjectSerializer<T> {

	/**
	 * Invoked on new storage creation -- chance to extend metadata with serializer
	 * own parameters.
	 */
	public RecordStorage.Metadata extend( final RecordStorage.Metadata metadata ) throws StorageException;

	/** @return record size in bytes. */
	public int recordSize();

	public void write( final T object,
	                   final DataOutput stream ) throws IOException, StorageException;

	public T read( final DataInput stream,
	               final T sharedHolder ) throws IOException, StorageException;
}
