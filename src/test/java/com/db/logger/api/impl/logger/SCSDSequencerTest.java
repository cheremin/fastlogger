package com.db.logger.api.impl.logger;

/**
 * @author ruslan
 *         created 10.12.13 at 1:59
 */
public class SCSDSequencerTest extends SequencerTestBase {
	@Override
	protected Sequencer createSequencer( final int size ) {
		return new SCSDSequencer( size );
	}
}
