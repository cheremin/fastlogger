package com.db.logger.io.storage;

/**
 * @author ruslan
 *         created 25.08.13 at 23:16
 */
public interface ObjectSerializerFactory<T> {
	/**
	 * Chance to setup serializer from metadata. Invoked on new storage creation, and
	 * on loading existent one. You can return different serializers from the method
	 * according to metadata content (e.g. version field), and returned serializer
	 * will be used to operate with storage -- this gives the ability to transparently
	 * support backward compatibility via set of serializers for different storage
	 * format versions.
	 * <p/>
	 * Created serializer expected to be stateless. It's better for it to be even
	 * immutable, although it is not required.
	 *
	 * @param newStorage is it serializer for create new empty storage (true) or load
	 *                   existent one. Some fields may be required in existent storage,
	 *                   but just set to defaults if omitted, for new one
	 */
	public ObjectSerializer<T> createBy( final RecordStorage.Metadata metadata,
	                                     final boolean newStorage );
}
