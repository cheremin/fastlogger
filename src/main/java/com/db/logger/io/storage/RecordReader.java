package com.db.logger.io.storage;

import gnu.trove.procedure.TObjectProcedure;

import java.io.IOException;

/**
 * @author cherrus
 *         created 11/14/13 at 10:18 AM
 */
public interface RecordReader<T> {
    /** @return metadata stored alongside the storage. Can't be modified */
    public RecordStorage.Metadata metadata();

    public int fetchRecords( final int startRecord,
                             final int maxRecordCount,
                             final TObjectProcedure<? super T> processor,
                             final T sharedDataHolder ) throws IOException;

    public long recordsCount();
}
