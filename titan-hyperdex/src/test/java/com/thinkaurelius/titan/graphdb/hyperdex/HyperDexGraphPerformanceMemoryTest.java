package com.thinkaurelius.titan.graphdb.hyperdex;

import com.thinkaurelius.titan.HyperDexStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class HyperDexGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    public HyperDexGraphPerformanceMemoryTest() {
        super(HyperDexStorageSetup.getPerformanceConfiguration());
    }

}
