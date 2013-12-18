package com.db.logger.benchmarks;

import com.db.logger.api.impl.logger.RecordHelper;
import com.db.logger.api.impl.logger.Sequencer;
import com.db.logger.api.impl.logger.buffer.ICircularLongsBuffer;
import org.openjdk.jmh.logic.BlackHole;

import static com.db.logger.api.impl.logger.RecordHelper.*;

/**
 * @author ruslan
 *         created 06.12.13 at 2:07
 */
public class ConsumingDrainer implements Sequencer.Drainer {

	private final ICircularLongsBuffer buffer;
	private final int spinsPerTurn;

	public ConsumingDrainer( final ICircularLongsBuffer buffer ) {
		this( buffer, 256 );
	}

	public ConsumingDrainer( final ICircularLongsBuffer buffer,
	                         final int spinsPerTurn ) {
		this.buffer = buffer;
		this.spinsPerTurn = spinsPerTurn;
	}

	private final BlackHole hole = new BlackHole();
	private int spinsAvailable;

	@Override
	public int available( final long startSequence,
	                      final long sentinelSequence ) {
		spinsAvailable = spinsPerTurn;//reset available spins count

		long pos = startSequence;
		for(; pos < sentinelSequence; pos++ ) {
			final long header = readHeader( pos );
			if( !isValidHeader( header ) ) {
				break;
			}
			final RecordHelper.RecordType type = type( header );
			final int formatId = formatId( header );
			final int argumentsCount = argumentsCount( header );

			buffer.put( pos, NOT_SET );
			for( int i = 1; i <= argumentsCount; i++ ) {
				hole.consume( buffer.get( pos + i ) );
				buffer.put( pos + i, NOT_SET );//needs to reclaim all!
			}

			pos += argumentsCount;
		}
		return ( int ) ( pos - startSequence );
	}

	private long readHeader( final long pos ) {
		for(; spinsAvailable >= 0; spinsAvailable-- ) {
			final long header = buffer.getVolatile( pos );
			if( isValidHeader( header ) ) {
				return header;
			}
		}
		return NOT_SET;
	}
}
