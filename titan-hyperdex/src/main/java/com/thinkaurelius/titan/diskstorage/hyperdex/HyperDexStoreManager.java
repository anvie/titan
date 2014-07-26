package com.thinkaurelius.titan.diskstorage.hyperdex;


import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.hyperdex.client.Client;
import org.hyperdex.client.HyperDexClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HyperDexStoreManager extends DistributedStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(HyperDexStoreManager.class);

    private final Map<String, HyperDexKeyValueStore> stores;
    private final String host;

    protected final StoreFeatures features;
    private final Client hyperdexClient;

    public HyperDexStoreManager(Configuration storageConfig) throws StorageException {
        super(storageConfig, storageConfig.getInt(GraphDatabaseConfiguration.PORT_KEY, 1982));

        stores = new HashMap<String, HyperDexKeyValueStore>();

        this.host = storageConfig.getString(GraphDatabaseConfiguration.HOSTNAME_KEY, "127.0.0.1");

        hyperdexClient = new Client(this.host, this.port);

        features = new StoreFeatures();
        features.supportsOrderedScan = true;
        features.supportsUnorderedScan = false;
        features.supportsBatchMutation = false;
        features.supportsTransactions = true;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = false;
        features.isKeyOrdered = true;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;
        features.supportsMultiQuery = false;
    }

    @Override
    public OrderedKeyValueStore openDatabase(String name) throws StorageException {

        if (stores.containsKey(name)){
            return stores.get(name);
        }

        log.trace("open database: " + name);


        HyperDexKeyValueStore store = new HyperDexKeyValueStore(name, this, hyperdexClient);
        stores.put(name, store);

        return store;
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreTransaction beginTransaction(StoreTxConfig config) throws StorageException {
        return new TransactionHandle(config);
    }

    @Override
    public void close() throws StorageException {
        if (this.hyperdexClient != null){
            if (!stores.isEmpty()){
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            }
            this.hyperdexClient.destroy();
        }
    }

    public void removeDatabase(HyperDexKeyValueStore db){
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        stores.remove(db.getName());
    }

    @Override
    public void clearStorage() throws StorageException {
//        throw new UnsupportedOperationException();
//        for (String key : stores.keySet()) {
//            HyperDexKeyValueStore store = stores.remove(key);
//        }
//
        try {
//            for (String key: stores.keySet()){
//                HyperDexKeyValueStore store = stores.remove(key);
//            }
            stores.clear();
            this.hyperdexClient.group_del(HyperDexKeyValueStore.SPACE_NAME, new HashMap<String,Object>());
        } catch (HyperDexClientException e) {
            throw new PermanentStorageException(e.getMessage());
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + "(" + this.host + ":" + this.port + ")";
    }

    private class TransactionHandle extends AbstractStoreTransaction {

        public TransactionHandle(final StoreTxConfig config) {
            super(config);
        }
    }
}
