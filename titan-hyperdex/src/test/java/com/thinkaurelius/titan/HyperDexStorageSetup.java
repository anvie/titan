package com.thinkaurelius.titan;


import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

public class HyperDexStorageSetup extends StorageSetup {

    public static Configuration getStorageConfiguration() {
        return getGraphBaseConfiguration();
    }

    public static Configuration getGraphConfiguration() {
        return getGraphBaseConfiguration();
    }

    public static Configuration getPerformanceConfiguration() {
        Configuration config = getGraphBaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY, false);
        config.addProperty(GraphDatabaseConfiguration.TX_CACHE_SIZE_KEY, 1000);
        return config;
    }

    public static Configuration getGraphBaseConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hyperdex");
        storage.addProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "10.0.0.3");
        storage.addProperty(GraphDatabaseConfiguration.PORT_KEY, 1982);
        return config;
    }


}
