package com.db.logger.io.storage;

/**
 * Abstraction for event storage. Storage is just a plain fixed-column table
 * with metadata (key-value pairs). Storage can be appended and read.
 *
 * TODO RC: implement rolled-storage adapter on the top of plain file based
 */