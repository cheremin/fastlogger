package com.db.logger.api.impl.logger.formatters;

import com.db.logger.api.FluentLogBuilder;
import net.jcip.annotations.Immutable;

/**
 * @author ruslan
 *         created 05.12.13 at 23:41
 */
@Immutable
public class DummyLogBuilder implements FluentLogBuilder {
	public static final DummyLogBuilder INSTANCE = new DummyLogBuilder();

	protected DummyLogBuilder() {
	}

	@Override
	public FluentLogBuilder with( final double value ) {
		return this;
	}

	@Override
	public FluentLogBuilder with( final long value ) {
		return this;
	}

	@Override
	public void submit() {

	}
}
