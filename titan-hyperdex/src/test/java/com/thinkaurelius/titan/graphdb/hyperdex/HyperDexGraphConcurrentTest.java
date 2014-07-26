package com.thinkaurelius.titan.graphdb.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class HyperDexGraphConcurrentTest extends TitanGraphConcurrentTest {

    public HyperDexGraphConcurrentTest() {
        super(HyperDexStorageSetup.getGraphConfiguration());
    }

}
