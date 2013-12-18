package com.db.logger.io.storage.impl;


import com.db.logger.io.storage.*;
import gnu.trove.procedure.TObjectProcedure;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.db.logger.io.storage.RecordStorage.OpenMode.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author cherrus
 *         created 8/13/13 at 11:12 AM
 */
public abstract class RecordStorageTestBase {

    protected abstract RecordStorage<Bean> storage( final ObjectSerializerFactory<Bean> serializerFactory ) throws Exception;

    protected abstract RecordStorage<Bean> storage( final RecordStorage.Configuration<Bean> configuration ) throws Exception;

    protected RecordStorage<Bean> storage;

    @Before
    public void setUp() throws Exception {
        storage = storage( BeanSerializer.FACTORY );
        if( storage.exists() ) {
            storage.remove();
        }
    }

    @After
    public void tearDown() throws Exception {
        if( storage.exists() ) {
            try {
                storage.remove();
            } catch( Exception e ) {
            }
        }
    }

    @Test
    public void initiallyStorageNotExists() throws Exception {
        assertFalse( storage.exists() );
    }

    @Test
    public void initiallyStorageNotOpened() throws Exception {
        assertFalse( storage.opened() );
    }

    @Test( expected = IllegalStateException.class )
    public void loadNonExistentStorageFails() throws Exception {
        storage.open( LOAD_EXISTENT, BaseMetadata.EMPTY );
    }

    @Test
    public void storageExistsAfterCreation() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );
        assertTrue( storage.exists() );
    }

    @Test
    public void storageNotExistsAfterDeletion() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );
        storage.remove();
        assertFalse( storage.exists() );
    }

    @Test
    public void storageNotOpenedAfterDeletion() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );
        storage.remove();
        assertFalse( storage.opened() );
    }

    @Test
    public void freshStorageInitiallyEmpty() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );
        final RecordsCollector counter = new RecordsCollector();
        storage.openForRead().fetchRecords(
                0, Integer.MAX_VALUE,
                counter,
                new Bean()
        );

        assertThat(
                counter.getRecords(),
                Matchers.<Bean>empty()
        );
    }

    @Test
    public void writtenItemsCanBeReadFromStorage() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );

        final Bean[] beans = {
                new Bean( 1L, 1.1 ),
                new Bean( 3L, 3.1 ),
                new Bean( 5L, 5.1 )
        };

        writeItems( beans );

        final RecordsCollector collector = new RecordsCollector();
        final int recordsRead = storage.openForRead().fetchRecords(
                0, Integer.MAX_VALUE,
                collector,
                new Bean()
        );

        assertThat(
                recordsRead,
                is( beans.length )
        );
        assertThat(
                collector.getRecords(),
                contains( beans )
        );
    }

    @Test
    public void writtenItemsCanBeCounted() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );

        final Bean[] beans = {
                new Bean( 1L, 1.1 ),
                new Bean( 3L, 3.1 ),
                new Bean( 5L, 5.1 )
        };

        writeItems( beans );

        assertThat(
                ( int ) storage.recordsCount(),
                is( beans.length )
        );
    }

    @Test
    public void itemsCanBeReadWithoutSharedHolderAsWell() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );

        final Bean[] beans = {
                new Bean( 1L, 1.1 ),
                new Bean( 3L, 3.1 ),
                new Bean( 5L, 5.1 )
        };

        writeItems( beans );

        final RecordsCollector collector = new RecordsCollector();
        storage.openForRead().fetchRecords(
                0, Integer.MAX_VALUE,
                collector,
                null
        );

        assertThat(
                collector.getRecords(),
                contains( beans )
        );
    }

    @Test
    public void itemsWrittenInSeveralStagesAppendedToStorage() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );

        final Bean[] beans = {
                new Bean( 1L, 1.1 ),
                new Bean( 3L, 3.1 ),
                new Bean( 5L, 5.1 )
        };

        writeItems( beans );
        writeItems( beans );

        final RecordsCollector collector = new RecordsCollector();
        final int recordsRead = storage.openForRead().fetchRecords(
                0, Integer.MAX_VALUE,
                collector,
                new Bean()
        );

        assertThat(
                recordsRead,
                is( beans.length * 2 )
        );
        assertThat(
                collector.getRecords(),
                contains( beans[0], beans[1], beans[2],
                        beans[0], beans[1], beans[2]
                )
        );
    }

    @Test
    public void limitedReadReturnItemsCountRequested() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );

        final Bean[] beans = {
                new Bean( 1L, 1.1 ),
                new Bean( 3L, 3.1 ),
                new Bean( 5L, 5.1 )
        };

        writeItems( beans );

        final RecordsCollector collector = new RecordsCollector();
        final int requestedItems = 2;
        final int recordsRead = storage.openForRead().fetchRecords(
                0, requestedItems,
                collector,
                new Bean()
        );

        assertThat(
                recordsRead,
                is( requestedItems )
        );
        assertThat(
                collector.getRecords(),
                contains( beans[0], beans[1] )
        );
    }

    @Test
    public void appendHugeDataAndReadInSeveralBatches() throws Exception {
        final Bean bean = new Bean( 0, 0 );
        final RecordWriter<Bean> writer = storage.openForAppend( CREATE_EMPTY, BaseMetadata.EMPTY );
        try {
            for( int i = 0; i < 1000000; i++ ) {
                bean.id = i;
                bean.value = i + 0.1;
                writer.append( bean );
            }
        } finally {
            writer.close();
        }

        final int batchSize = 100000;
        for( int i = 0; i < 10; i++ ) {
            final int startIndex = i * batchSize;
            final RecordReader<Bean> reader = storage.openForRead();
            reader.fetchRecords( startIndex, batchSize, new TObjectProcedure<Bean>() {
                int counter = startIndex;

                @Override
                public boolean execute( final Bean b ) {
                    assertEquals(
                            "batchStart=" + startIndex,
                            counter,
                            b.id
                    );
                    assertEquals(
                            "batchStart=" + startIndex,
                            counter + 0.1,
                            b.value,
                            0
                    );
                    counter++;
                    return true;
                }
            }, new Bean() );
        }
    }

    @Test( expected = IllegalStateException.class )
    public void createSecondWriterOnSameStorageFails() throws Exception {
        final RecordWriter<Bean> w1 = storage.openForAppend( CREATE_EMPTY, BaseMetadata.EMPTY );
        final RecordWriter<Bean> w2 = storage.openForAppend( CREATE_EMPTY, BaseMetadata.EMPTY );
    }

    @Test
    public void createSecondWriterIsOkIfFirstOneClosed() throws Exception {
        storage.open( CREATE_EMPTY, BaseMetadata.EMPTY );
        final RecordWriter<Bean> w1 = storage.openForAppend( CREATE_EMPTY, BaseMetadata.EMPTY );
        w1.close();
        final RecordWriter<Bean> w2 = storage.openForAppend( CREATE_EMPTY, BaseMetadata.EMPTY );
        w2.close();
    }

    @Test
    public void metadataStoredOnStorageCreationAndRestoredOnLoadExistent() throws Exception {
        final RecordStorage.Configuration<Bean> configuration = storage.configuration();

        //force to store metadata
        storage.open(
                CREATE_EMPTY,
                BaseMetadata.EMPTY
                        .extend( "external:key", "external value" )
        );

        //new storage instance with same params
        final RecordStorage<Bean> newStorage = storage( configuration );
        newStorage.open(
                LOAD_EXISTENT,
                BaseMetadata.EMPTY
        );

        final RecordStorage.Metadata readMetadata = newStorage.metadata();

        assertEquals( "1", readMetadata.value( "serializer:version" ) );
        assertEquals( "serializer value", readMetadata.value( "serializer:key" ) );
        assertEquals( "external value", readMetadata.value( "external:key" ) );
    }

    //TODO check serializer setup/extends called as specified

    /*======================= INFRA-STRUCTURE ==================================*/

    private void writeItems( final Bean... beans ) throws IOException {
        final RecordWriter<Bean> writer = storage.openForAppend( CREATE_IF_NOT_EXISTS, BaseMetadata.EMPTY );
        try {
            for( final Bean bean : beans ) {
                writer.append( bean );
            }
        } finally {
            writer.close();
        }
    }


    public static class BeanSerializer implements ObjectSerializerNIO<Bean> {
        public static final ObjectSerializerFactory<Bean> FACTORY = new ObjectSerializerFactory<Bean>() {
            @Override
            public BeanSerializer createBy( final RecordStorage.Metadata metadata, final boolean newStorage ) {
                return new BeanSerializer( metadata );
            }
        };

        private final RecordStorage.Metadata setupMetadata;

        private BeanSerializer( final RecordStorage.Metadata setupMetadata ) {
            this.setupMetadata = setupMetadata;
        }


        @Override
        public RecordStorage.Metadata extend( final RecordStorage.Metadata metadata ) throws StorageException {
            return metadata
                    .extend( "serializer:version", "1" )
                    .extend( "serializer:key", "serializer value" );
        }

        @Override
        public int recordSize() {
            return ( Long.SIZE + Double.SIZE ) / Byte.SIZE;
        }

        @Override
        public void write( final Bean record,
                           final DataOutput stream ) throws IOException, StorageException {
            stream.writeLong( record.id );
            stream.writeDouble( record.value );
        }

        @Override
        public Bean read( final DataInput stream,
                          final Bean sharedHolder ) throws IOException, StorageException {
            final Bean record;
            if( sharedHolder != null ) {
                record = sharedHolder;
            } else {
                record = new Bean();
            }
            record.id = stream.readLong();
            record.value = stream.readDouble();
            return record;
        }

        @Override
        public void write( final Bean record,
                           final ByteBuffer buffer ) throws IOException, StorageException {
            buffer.putLong( record.id );
            buffer.putDouble( record.value );
        }

        @Override
        public Bean read( final ByteBuffer buffer,
                          final Bean sharedHolder ) throws IOException, StorageException {
            final Bean record;
            if( sharedHolder != null ) {
                record = sharedHolder;
            } else {
                record = new Bean();
            }
            record.id = buffer.getLong();
            record.value = buffer.getDouble();
            return record;
        }
    }

    public static class Bean implements Cloneable {
        public long id;
        public double value;

        public Bean() {
        }

        public Bean( final long id,
                     final double value ) {
            this.id = id;
            this.value = value;
        }

        @Override
        public boolean equals( final Object o ) {
            if( this == o ) {
                return true;
            }
            if( o == null || getClass() != o.getClass() ) {
                return false;
            }

            final Bean bean = ( Bean ) o;

            if( id != bean.id ) {
                return false;
            }
            if( Double.compare( bean.value, value ) != 0 ) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = ( int ) ( id ^ ( id >>> 32 ) );
            temp = value != +0.0d ? Double.doubleToLongBits( value ) : 0L;
            result = 31 * result + ( int ) ( temp ^ ( temp >>> 32 ) );
            return result;
        }

        @Override
        public String toString() {
            return "Bean{" + id + " => " + value + '}';
        }

        @Override
        public Bean clone() {
            try {
                return ( Bean ) super.clone();
            } catch( CloneNotSupportedException e ) {
                throw new AssertionError( "Code bug:" + e.getMessage() );
            }
        }
    }

    private static class RecordsCollector implements TObjectProcedure<Bean> {
        private final List<Bean> items = new ArrayList<Bean>();

        @Override
        public boolean execute( final Bean record ) {
            items.add( record.clone() );
            return true;
        }

        public List<Bean> getRecords() {
            return items;
        }
    }

}
