package com.db.logger.api.impl.logger.formatters;

import com.db.logger.api.LogMessage;
import com.db.logger.api.impl.logger.RingBuffer;
import net.jcip.annotations.NotThreadSafe;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author ruslan
 *         created 20.11.13 at 23:48
 */
@NotThreadSafe
public final class SimpleLogMessage extends AbstractLogBuilder implements LogMessage {

	private final String format;
	private final int formatId;

	private final int argumentsCount;


	public SimpleLogMessage( final String format,
	                         final int formatId,
	                         final int argumentsCount,
	                         final RingBuffer ringBuffer ) {
		super( ringBuffer );
		this.format = format;

		this.formatId = formatId;
		this.argumentsCount = argumentsCount;
	}

	public int formatId() {
		return formatId;
	}

	@Override
	public String format() {
		return format;
	}

	@Override
	public int argumentsCount() {
		return argumentsCount;
	}
}
