package com.db.logger.api.impl.logger.formatters;

import com.db.logger.api.FluentLogBuilder;
import com.db.logger.api.impl.logger.RingBuffer;
import com.db.logger.api.impl.logger.RecordHelper;
import net.jcip.annotations.NotThreadSafe;

import static com.db.logger.api.impl.logger.MCSDSequencer.INVALID_INDEX;
import static com.db.logger.api.impl.logger.RecordHelper.RecordType.LOG_RECORD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * @author ruslan
 *         created 05.12.13 at 0:52
 */
@NotThreadSafe
public abstract class AbstractLogBuilder implements FluentLogBuilder {
	public static final int NOT_SET = -1;

	private final RingBuffer buffer;

	protected int argumentIndex = NOT_SET;
	protected long position = INVALID_INDEX;

	public AbstractLogBuilder( final RingBuffer buffer ) {
		checkArgument( buffer != null, "buffer can't be null" );
		this.buffer = buffer;
	}

	@Override
	public FluentLogBuilder with( final double value ) {
		return with( Double.doubleToLongBits( value ) );
	}

	@Override
	public FluentLogBuilder with( final long value ) {
		ensureStarted();
//		checkState(
//				argumentIndex < argumentsCount(),
//				"Only %s arguments allowed but %s (%s) here",
//				argumentsCount(), argumentIndex, value
//		);

		argumentIndex++;//0 reserved for header!
		buffer.buffer().put(
				position + argumentIndex,
				value
		);

		return this;
	}

	@Override
	public void submit() {
		ensureStarted();
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
			reset();
		}
	}

	protected void reset() {
		argumentIndex = NOT_SET;
		position = INVALID_INDEX;
	}

	protected void ensureStarted() {
		if( position == INVALID_INDEX ) {
			position = buffer.claim( argumentsCount() + 1 );//1 for header
			checkState( position != INVALID_INDEX, "Claim failed" );

			argumentIndex = 0;
		}
	}

	protected abstract int formatId();

	protected abstract int argumentsCount();
}
