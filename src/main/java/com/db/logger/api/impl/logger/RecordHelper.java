package com.db.logger.api.impl.logger;

/**
 * @author ruslan
 *         created 22.11.13 at 0:12
 */
public abstract class RecordHelper {

	public static final long NOT_SET = -1L;

	public enum RecordType {
		LOG_RECORD( 1 ),
		TIMESTAMP( 2 );
		private final byte id;

		private RecordType( final int id ) {
			this.id = ( byte ) id;
		}

		public byte id() {
			return id;
		}
	}

	public static long header( final RecordType type,
	                           final int formatId,
	                           final int argumentsCount ) {
		//[type x 15][formatId x 32][argumentsCount x 16]
		return ( ( ( long ) type.id() ) << 48 )
				| ( formatId << 16 )
				| argumentsCount;
	}

	public static boolean isValidHeader( final long header ) {
		return header > 0;
	}

	public static RecordType type( final long header ) {
		final int typeNo = ( int ) ( header >> 48 );
		switch( typeNo ) {
			case 1:
				return RecordType.LOG_RECORD;
			case 2:
				return RecordType.TIMESTAMP;
			default:
				throw new IllegalArgumentException( "type " + typeNo + " is unknown" );
		}
	}

	public static int formatId( final long header ) {
		return ( int ) ( ( header >> 16 ) & 0xFFFFFFFFL );
	}

	public static int argumentsCount( final long header ) {
		return ( int ) ( header & 0xFFFFL );
	}
}
