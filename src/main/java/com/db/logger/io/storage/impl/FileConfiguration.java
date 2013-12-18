package com.db.logger.io.storage.impl;

import java.io.File;

import com.db.logger.io.storage.ObjectSerializerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author cherrus
 *         created 8/15/13 at 11:39 AM
 */
public class FileConfiguration<T> extends BaseConfiguration<T> {
	private static final int DEFAULT_BUFFERED_RECORDS = ( 1 << 13 );

	private final File file;
	/** in records, not in bytes! */
	private final int readBufferSize;
	/** in records, not in bytes! */
	private final int writeBufferSize;

	public FileConfiguration( final ObjectSerializerFactory<T> serializerFactory,
	                          final File file ) {
		this( serializerFactory, file, DEFAULT_BUFFERED_RECORDS, DEFAULT_BUFFERED_RECORDS );
	}

	public FileConfiguration( final ObjectSerializerFactory<T> serializerFactory,
	                          final File file,
	                          final int readBufferSize,
	                          final int writeBufferSize ) {
		super( serializerFactory );
		checkArgument( file != null, "file can't be null" );
		checkArgument( readBufferSize > 0, "readBufferSize(%s) must be >0", readBufferSize );
		checkArgument( writeBufferSize > 0, "writeBufferSize(%s) must be >0", writeBufferSize );

		this.file = file;
		this.readBufferSize = readBufferSize;
		this.writeBufferSize = writeBufferSize;
	}

	public File file() {
		return file;
	}

	/** in records, not in bytes! */
	public int readBufferSize() {
		return readBufferSize;
	}

	/** in records, not in bytes! */
	public int writeBufferSize() {
		return writeBufferSize;
	}
}
