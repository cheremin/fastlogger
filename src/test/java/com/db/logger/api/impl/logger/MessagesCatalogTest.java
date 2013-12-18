package com.db.logger.api.impl.logger;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author ruslan
 *         created 10.12.13 at 21:25
 */
public class MessagesCatalogTest {
	public static final String[] MESSAGES = {
			"",
			"Abc",
			"Abc %d",
			"Abc %d %f %s",
			"Abc %d %f %s 2"
	};

	@Test
	public void repeatedLookupsReturnSameInstance() throws Exception {
		final MessagesCatalog catalog = new MessagesCatalog( 32 );
		for( final String message : MESSAGES ) {
			final MessageInfo info = catalog.lookupMessageInfo( message );
			final MessageInfo info2 = catalog.lookupMessageInfo( message );
			assertSame( info, info2 );
		}
	}

	@Test
	public void messageIdsAreUnique() throws Exception {
		final MessagesCatalog catalog = new MessagesCatalog( 32 );
		final TIntSet ids = new TIntHashSet();
		for( final String message : MESSAGES ) {
			final MessageInfo info = catalog.lookupMessageInfo( message );
			ids.add( info.formatId );
		}
		assertEquals( MESSAGES.length, ids.size() );
	}
}
