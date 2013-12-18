package com.db.logger.io.storage.impl;

import java.io.File;

import static com.db.logger.io.storage.RecordStorage.OpenMode.CREATE_EMPTY;

/**
 * @author cherrus
 *         created 8/15/13 at 6:19 PM
 */
public class StorageBenchmark {
//    public static void main( final String[] args ) throws Exception {
//        final int maxTagsCount = 4;
//        final ObjectSerializerFactory<EventRecord> serializerFactory = RecordSerializer.FACTORY;
//
//        final int readBufferSize = 4 * 1024;
//        final int writeBufferSize = 4 * 1024;
//        final FileConfiguration<EventRecord> configuration = new FileConfiguration<EventRecord>(
//                serializerFactory,
//                new File( "storage/events" ),
//                readBufferSize,
//                writeBufferSize
//        );
//        final RecordStorage<EventRecord> storage = new FileRecordStorageNIO<EventRecord>(
//                configuration
//        );
//        final EventRecord record = new EventRecord( 1, maxTagsCount, 0 );
//        final int recordsCount = 1 << 26;
//        final RecordWriter<EventRecord> writer = storage.openForAppend( CREATE_EMPTY, BaseMetadata.EMPTY );
//        try {
//            final long startedAtMs = System.currentTimeMillis();
//            for( int i = 0; i < recordsCount; i++ ) {
//                record.eventId = i;
//                record.locationId = i;
//                record.timestampNanos = i;
//                record.causeIds = new long[]{ i };
//                for( int j = 0; j < record.tags.length; j++ ) {
//                    record.tags[j] = j;
//
//                }
//                writer.append( record );
//            }
//            final long finishedAtMs = System.currentTimeMillis();
//            final long elapsed = finishedAtMs - startedAtMs;
//            System.out.printf(
//                    "Write %d ms/%d ~ %d rec/ms\n",
//                    elapsed,
//                    recordsCount,
//                    ( recordsCount / elapsed )
//            );
//        } finally {
//            writer.close();
//        }
//
//
//        {
//            final long startedAtMs = System.currentTimeMillis();
//            storage.openForRead().fetchRecords(
//                    0, Integer.MAX_VALUE,
//                    new TObjectProcedure<EventRecord>() {
//                        @Override
//                        public boolean execute( final EventRecord record ) {
//                            return true;
//                        }
//                    },
//                    new EventRecord()
//            );
//
//            final long finishedAtMs = System.currentTimeMillis();
//            final long elapsed = finishedAtMs - startedAtMs;
//            System.out.printf(
//                    "Read %d ms/%d ~ %d rec/ms\n",
//                    elapsed,
//                    recordsCount,
//                    ( recordsCount / elapsed )
//            );
//        }
//    }
}
