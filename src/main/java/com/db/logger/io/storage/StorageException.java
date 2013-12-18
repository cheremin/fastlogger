package com.db.logger.io.storage;


/**
 * @author cherrus
 *         created 8/12/13 at 6:34 PM
 */
public class StorageException extends RuntimeException {
	public StorageException( final String message ) {
		super( message );
	}

	public StorageException( final String message,
	                         final Throwable cause ) {
		super( message, cause );
	}
}
