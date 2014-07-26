package com.thinkaurelius.titan.diskstorage.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

public class HyperDexHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = HyperDexStorageSetup.getStorageConfiguration()
                .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        HyperDexStoreManager sm = new HyperDexStoreManager(config);

        // prefixed store doesn't support scan, because prefix is hash of a key which makes it un-ordered
        sm.features.supportsUnorderedScan = false;
        sm.features.supportsOrderedScan = false;

        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Test
    @Override
    public void testGetKeysWithKeyRange() {
        // Requires ordered keys, but we are using hash prefix
    }

    @Test
    @Override
    public void testOrderedGetKeysRespectsKeyLimit() {
        // Requires ordered keys, but we are using hash prefix
    }
}
