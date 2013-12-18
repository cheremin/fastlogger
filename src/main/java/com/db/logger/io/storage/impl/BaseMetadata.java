package com.db.logger.io.storage.impl;

import com.db.logger.io.storage.RecordStorage;
import net.jcip.annotations.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ruslan
 *         created 12.08.13 at 0:01
 */
@Immutable
public class BaseMetadata implements RecordStorage.Metadata {
    private final HashMap<String, String> values = new HashMap<String, String>();

    public static final BaseMetadata EMPTY = new BaseMetadata( Collections.<String, String>emptyMap() );

    public BaseMetadata( final Map<String, String> values ) {
        this.values.putAll( values );
    }

    @Override
    public String[] keys() {
        return values.keySet().toArray( new String[values.size()] );
    }

    @Override
    public String value( final String key ) {
        return values.get( key );
    }

    @Override
    public RecordStorage.Metadata extend( final String key,
                                          final String value ) {
        final HashMap<String, String> _values = new HashMap<String, String>( values );
        _values.put( key, value );
        return new BaseMetadata( _values );
    }

    @Override
    public String toString() {
        return "BaseMetadata[" + values + ']';
    }
}
