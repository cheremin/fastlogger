package com.db.logger.api;


/**
 * @author ruslan
 *         created 20.11.13 at 23:51
 */
public interface FluentLogBuilder {
	public FluentLogBuilder with( final double value );

	public FluentLogBuilder with( final long value );

	public void submit();
}
