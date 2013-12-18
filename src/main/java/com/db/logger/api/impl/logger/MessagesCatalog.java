package com.db.logger.api.impl.logger;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author ruslan
 *         created 10.12.13 at 19:22
 */
public class MessagesCatalog {
	private static final Object FREE = null;

	private final MessageInfo[] entries;
	private int id = 0;


	public MessagesCatalog( final int size ) {
		checkArgument( size > 1, "size[%s] must be >1", size );
		checkArgument( Integer.bitCount( size ) == 1, "size[%s] must be 2^n", size );

		entries = new MessageInfo[size];
	}


	public MessageInfo lookupMessageInfo( final String messageFormat ) {
		final int hash = messageFormat.hashCode() & 0x7fffffff;

		final int length = entries.length;
		final int mask = length - 1;
		final int startIndex = hash;

		for( int i = 0; i < length; i++ ) {
			final int index = ( startIndex + i ) & mask;
			final MessageInfo cur = entries[index];
			if( cur == FREE ) {
				//cache miss
				return recheckAndStore( messageFormat, hash );
			} else if( cur.format.equals( messageFormat ) ) {
				//cache hit
				return cur;
			}
		}

		throw new IllegalStateException( "Table overloaded" );
	}

	private synchronized MessageInfo recheckAndStore( final String format,
	                                                  final int hash ) {
		//we restart search from start since anybody could race with us
		final int length = entries.length;
		final int mask = length - 1;
		final int startIndex = hash;

		for( int i = 0; i < length; i++ ) {
			final int index = ( startIndex + i ) & mask;
			final MessageInfo cur = entries[index];
			if( cur == FREE ) {
				//cache miss
				return createAndStore( format, index );
			} else if( cur.format.equals( format ) ) {
				//cache hit
				return cur;
			}
		}
		throw new IllegalStateException( "Table overloaded" );
	}

	private MessageInfo createAndStore( final String format,
	                                    final int index ) {
		id++;
		final MessageInfo messageInfo = new MessageInfo(
				format,
				calculateArgumentsCount( format ),
				id
		);
		entries[index] = messageInfo;
		return messageInfo;
	}

	private static int calculateArgumentsCount( final String formatMessage ) {
		int argumentsCount = 0;
		for( int i = 0; i < formatMessage.length(); i++ ) {
			if( formatMessage.charAt( i ) == '%' ) {//TODO RC: not safe! '%%' is not placeholder
				argumentsCount++;
			}
		}
		return argumentsCount;
	}


	public synchronized void purge() {
		Arrays.fill( entries, FREE );
		id = 0;
	}
}
