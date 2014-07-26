package com.thinkaurelius.titan.graphdb.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class HyperDexGraphTest extends TitanGraphTest {

    public HyperDexGraphTest() {
        super(HyperDexStorageSetup.getGraphConfiguration());
    }

}
