package com.thinkaurelius.titan.graphdb.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.graphdb.SpeedComparisonPerformanceTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HyperDexSpeedComparisonPerformanceTest extends SpeedComparisonPerformanceTest {
    
    public HyperDexSpeedComparisonPerformanceTest() {
        super(getConfiguration());
    }
    
    private static final Configuration getConfiguration() {
        Configuration config = HyperDexStorageSetup.getGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE)
                .addProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY,false);
        return config;
    }
}
