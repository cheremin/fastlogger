package com.db.logger.api;

/**
 * @author ruslan
 *         created 20.11.13 at 23:05
 */
public interface LogMessage {
	public String format();

	public int argumentsCount();

	public FluentLogBuilder with( final double value );

	public FluentLogBuilder with( final long value );

	public void submit();

}
