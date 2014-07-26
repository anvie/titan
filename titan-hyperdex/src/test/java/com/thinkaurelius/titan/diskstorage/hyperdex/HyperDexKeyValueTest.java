package com.thinkaurelius.titan.diskstorage.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;


public class HyperDexKeyValueTest extends KeyValueStoreTest {

    private String storeName = "titan";

    @Override
    public void open() throws StorageException {
        manager = openStorageManager();
        store = manager.openDatabase(storeName);
        tx = manager.beginTransaction(new StoreTxConfig());
    }

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws StorageException {
        return new HyperDexStoreManager(HyperDexStorageSetup.getStorageConfiguration()
            .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
    }


    @Test
    public void testGet(){

    }

}
