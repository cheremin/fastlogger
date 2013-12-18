package com.db.logger.io.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Optional interface to implement by serializer which suggest NIO based
 * methods.
 *
 * @author ruslan
 *         created 11.08.13 at 14:16
 */
public interface ObjectSerializerNIO<T> extends ObjectSerializer<T> {

	/**
	 * Invoked on new storage creation -- chance to extend metadata with
	 * serializer own parameters.
	 */
	public RecordStorage.Metadata extend( final RecordStorage.Metadata metadata ) throws StorageException;

	public int recordSize();

	public void write( final T object,
	                   final ByteBuffer target ) throws IOException, StorageException;

	public T read( final ByteBuffer source,
	               final T sharedHolder ) throws IOException, StorageException;
}
