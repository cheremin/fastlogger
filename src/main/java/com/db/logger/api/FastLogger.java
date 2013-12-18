package com.db.logger.api;

/**
 * @author cherrus
 *         created 7/3/12 at 6:05 PM
 */
public interface FastLogger {

	public LogMessage messageSimple( final String messageFormat );

	public LogMessage messageThreadLocal( final String messageFormat );

	public LogMessage log( final String messageFormat );

}
