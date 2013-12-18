package com.db.logger.api.impl.logger;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.google.common.base.Throwables;
import sun.misc.Unsafe;

/**
 * @author ruslan
 *         created 14.09.13 at 11:58
 */
public class UnsafeHelper {
	private static final Unsafe unsafe;


	static {
		unsafe = AccessController.doPrivileged(
				new PrivilegedAction<Unsafe>() {
					@Override
					public Unsafe run() {
						try {
							final Field f = Unsafe.class.getDeclaredField( "theUnsafe" );
							f.setAccessible( true );
							return ( Unsafe ) f.get( null );
						} catch( Exception e ) {
							throw Throwables.propagate( e );
						}
					}
				} );
	}

	public static Unsafe unsafe() {
		return unsafe;
	}
}