package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Assert;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HyperDexBlueprintsTest extends TitanBlueprintsTest {


    @Override
    public Graph generateGraph() {
        return generateGraph("standard");
    }

    @Override
    public Graph generateGraph(String uid) {
//        String dir = BerkeleyJeStorageSetup.getHomeDir(uid);
//        System.out.println("Opening graph in: " + dir);
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "10.0.0.3");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hyperdex");
        Graph graph = TitanFactory.open(config);
        return graph;
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return true;
    }

    @Override
    public void cleanUp() throws StorageException {

    }


    @Override
    public void startUp() {
        //Nothing
    }

    @Override
    public void shutDown() {

    }
}
