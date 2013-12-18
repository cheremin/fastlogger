package com.db.logger.api.impl.logger.formatters;

import com.db.logger.api.LogMessage;
import com.db.logger.api.FluentLogBuilder;
import com.db.logger.api.impl.logger.RingBuffer;
import com.db.logger.api.impl.logger.MessageInfo;
import com.db.logger.api.impl.logger.RecordHelper;

import static com.db.logger.api.impl.logger.formatters.AbstractLogBuilder.NOT_SET;
import static com.db.logger.api.impl.logger.MCSDSequencer.INVALID_INDEX;
import static com.db.logger.api.impl.logger.RecordHelper.RecordType.LOG_RECORD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * @author ruslan
 *         created 20.11.13 at 23:48
 */
public final class RawLogMessage implements LogMessage, FluentLogBuilder {

	private final RingBuffer buffer;

	private int argumentIndex = NOT_SET;
	private long position = INVALID_INDEX;

	private String format;
	private int formatId;

	private int argumentsCount;

	public RawLogMessage( final RingBuffer ringBuffer ) {
		this.buffer = ringBuffer;
	}

	public RawLogMessage setup( final MessageInfo messageInfo ) {
		checkArgument( messageInfo != null, "messageInfo can't be null" );
		this.format = messageInfo.format;
		this.formatId = messageInfo.formatId;
		this.argumentsCount = messageInfo.argumentsCount;
		return this;
	}

	public RawLogMessage start() {
		checkState( argumentIndex == NOT_SET, "Submit first!" );
		position = buffer.claim( argumentsCount + 1 );//1 for header
		checkState( position != INVALID_INDEX, "Can't claim position" );
		argumentIndex = 0;
		return this;
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


	@Override
	public FluentLogBuilder with( final long value ) {
		checkState( position != INVALID_INDEX, "Not started" );
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
		return with( Double.doubleToLongBits( value ) );
	}

	@Override
	public void submit() {
		checkState( position != INVALID_INDEX, "Not started" );
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
