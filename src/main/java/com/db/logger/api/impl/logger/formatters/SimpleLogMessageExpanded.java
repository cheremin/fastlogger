package com.db.logger.api.impl.logger.formatters;

import com.db.logger.api.FluentLogBuilder;
import com.db.logger.api.LogMessage;
import com.db.logger.api.impl.logger.RingBuffer;
import com.db.logger.api.impl.logger.RecordHelper;

import static com.db.logger.api.impl.logger.MCSDSequencer.INVALID_INDEX;
import static com.db.logger.api.impl.logger.RecordHelper.RecordType.LOG_RECORD;
import static com.db.logger.api.impl.logger.formatters.AbstractLogBuilder.NOT_SET;
import static com.google.common.base.Preconditions.checkState;

/**
 * TODO Unsafe!
 *
 * @author ruslan
 *         created 20.11.13 at 23:48
 */
public final class SimpleLogMessageExpanded implements LogMessage, FluentLogBuilder {

	private final RingBuffer buffer;

	private int argumentIndex = NOT_SET;
	private long position = INVALID_INDEX;

	private final String format;
	private final int formatId;

	private final int argumentsCount;

	public SimpleLogMessageExpanded( final String format,
	                                 final int formatId,
	                                 final int argumentsCount,
	                                 final RingBuffer ringBuffer ) {
		this.buffer = ringBuffer;
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

	protected void ensureStarted() {
		if( position == INVALID_INDEX ) {
			position = buffer.claim( argumentsCount() + 1 );//1 for header
			checkState( position != INVALID_INDEX, "Claim failed" );

			argumentIndex = 0;
		}
	}


	@Override
	public FluentLogBuilder with( final long value ) {
//		ensureStarted();
		//		checkState(
		//				argumentIndex < argumentsCount(),
		//				"Only %s arguments allowed but %s is",
		//				argumentsCount(), argumentIndex
		//		);

		argumentIndex++;//0 reserved for header!
		buffer.buffer().put(
				position + argumentIndex,
				value
		);

		return this;
	}

	@Override
	public FluentLogBuilder with( final double value ) {
		ensureStarted();
		return with( Double.doubleToLongBits( value ) );
	}

	@Override
	public void submit() {
//		ensureStarted();
		try {
			//			checkState(
			//					argumentIndex == argumentsCount(),
			//					"early submit: %s < %s",
			//					argumentIndex, argumentsCount()
			//			);

			final long header = RecordHelper.header(
					LOG_RECORD,
					formatId(),
					argumentsCount()
			);
			buffer.buffer().putOrdered( position, header );
		} finally {
			argumentIndex = NOT_SET;
			position = INVALID_INDEX;
		}
	}

}
