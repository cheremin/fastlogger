package com.db.logger.io.storage.impl;

import com.db.logger.io.storage.ObjectSerializerFactory;
import com.db.logger.io.storage.RecordStorage;
import net.jcip.annotations.Immutable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author cherrus
 *         created 8/12/13 at 6:49 PM
 */
@Immutable
public class BaseConfiguration<T> implements RecordStorage.Configuration<T> {
	private final ObjectSerializerFactory<T> serializerFactory;

	public BaseConfiguration( final ObjectSerializerFactory<T> serializerFactory ) {
		checkArgument( serializerFactory != null, "serializerFactory can't be null" );
		this.serializerFactory = serializerFactory;
	}

	@Override
	public ObjectSerializerFactory<T> serializerFactory() {
		return serializerFactory;
	}
}
