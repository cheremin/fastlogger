package com.db.logger.io.storage.impl;

import java.io.File;
import java.io.IOException;

import com.db.logger.io.storage.ObjectSerializerFactory;
import com.db.logger.io.storage.RecordStorage;

/**
 * @author cherrus
 *         created 8/13/13 at 11:18 AM
 */
public class FileRecordStorageNIOTest extends RecordStorageTestBase {
	@Override
	protected RecordStorage<Bean> storage( final ObjectSerializerFactory<Bean> serializerFactory ) throws IOException {
		final File file = File.createTempFile( "record-storage", "test" );
		return new FileRecordStorageNIO<Bean>(
				new FileConfiguration<Bean>( serializerFactory, file )
		);
	}

	@Override
	protected RecordStorage<Bean> storage( final RecordStorage.Configuration<Bean> configuration ) throws Exception {
		return new FileRecordStorageNIO<Bean>(
				( FileConfiguration ) configuration
		);
	}
}
