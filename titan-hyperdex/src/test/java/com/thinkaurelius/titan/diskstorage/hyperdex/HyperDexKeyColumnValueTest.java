package com.thinkaurelius.titan.diskstorage.hyperdex;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;


public class HyperDexKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
//        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyJeStorageSetup.getBerkeleyJEStorageConfiguration());
        HyperDexStoreManager sm = new HyperDexStoreManager(
                HyperDexStorageSetup.getStorageConfiguration()
                        .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
