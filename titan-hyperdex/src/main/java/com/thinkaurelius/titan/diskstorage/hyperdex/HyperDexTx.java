package com.thinkaurelius.titan.diskstorage.hyperdex;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HyperDexTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(HyperDexTx.class);


    public HyperDexTx(StoreTxConfig config) {
        super(config);
    }

    @Override
    public void commit() throws StorageException {
        super.commit();
    }

    @Override
    public void rollback() throws StorageException {
        super.rollback();
    }

    @Override
    public void flush() throws StorageException {
        super.flush();
    }
}
