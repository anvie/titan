package com.thinkaurelius.titan.graphdb.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.CLIENT_ONLY_KEY;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.LOCAL_MODE_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ElasticSearchHyperDexTest extends TitanIndexTest {

    public ElasticSearchHyperDexTest() {
        super(getElasticSearchBDBConfig(), true, true, true);
    }

    public static final Configuration getElasticSearchBDBConfig() {
//        BaseConfiguration config = new BaseConfiguration();
//        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "com.thinkaurelius.titan.diskstorage.hyperdex.HyperDexStoreManager");
//        config.subset(STORAGE_NAMESPACE).addProperty(HOSTNAME_KEY, "127.0.0.1");
//        config.subset(STORAGE_NAMESPACE).addProperty(PORT_KEY, 1982);
        Configuration config = HyperDexStorageSetup.getGraphBaseConfiguration();
//        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir());
        //Add index
        Configuration sub = config.subset(STORAGE_NAMESPACE).subset(INDEX_NAMESPACE).subset(INDEX);
        sub.setProperty(INDEX_BACKEND_KEY,"com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex");
        sub.setProperty(LOCAL_MODE_KEY,true);
        sub.setProperty(CLIENT_ONLY_KEY,false);
        sub.setProperty(STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("es"));
//        System.out.println(GraphDatabaseConfiguration.toString(config));
        return config;
    }


    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }
}