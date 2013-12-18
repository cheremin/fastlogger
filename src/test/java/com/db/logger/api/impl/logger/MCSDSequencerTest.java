package com.db.logger.api.impl.logger;

/**
 * @author cherrus
 *         created 7/4/12 at 4:25 PM
 */
public class MCSDSequencerTest extends SequencerTestBase {
	@Override
	protected Sequencer createSequencer( final int size ) {
		return new MCSDSequencer( size );
	}
}
