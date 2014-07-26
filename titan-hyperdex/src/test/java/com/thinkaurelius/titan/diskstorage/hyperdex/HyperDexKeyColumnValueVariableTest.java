package com.thinkaurelius.titan.diskstorage.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;


public class HyperDexKeyColumnValueVariableTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = HyperDexStorageSetup.getStorageConfiguration();
        HyperDexStoreManager sm = new HyperDexStoreManager(config
                .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
