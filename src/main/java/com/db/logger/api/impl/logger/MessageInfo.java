package com.db.logger.api.impl.logger;

/**
 * @author ruslan
 *         created 07.12.13 at 16:25
 */
public final class MessageInfo {
	public final String format;
	public final int argumentsCount;
	public final int formatId;

	public MessageInfo( final String format,
	                    final int argumentsCount,
	                    final int formatId ) {
		this.format = format;
		this.argumentsCount = argumentsCount;
		this.formatId = formatId;
	}
}
