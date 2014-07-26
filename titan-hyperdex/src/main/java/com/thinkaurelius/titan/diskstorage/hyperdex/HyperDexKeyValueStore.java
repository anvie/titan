package com.thinkaurelius.titan.diskstorage.hyperdex;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.apache.commons.codec.digest.DigestUtils;
import org.hyperdex.client.*;
import org.hyperdex.client.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HyperDexKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(HyperDexKeyValueStore.class);
    private static final String UTF_8 = StandardCharsets.UTF_8.name();
    private static final long idOffset = 1000;
    static final String SPACE_NAME = "titan";

    private final String name;
    private final HyperDexStoreManager manager;
    private final Client hyperdexClient;
//    private static final String STORE_KEY = "vid";
    private static final String KEY = "_key";
    private static final String VALUE_KEY = "value";
    private static final List<String> SELECT_ATTRS = Arrays.asList(VALUE_KEY);

    public HyperDexKeyValueStore(String name, HyperDexStoreManager manager, Client hyperdexClient) {
        this.name = name;
        this.manager = manager;
        this.hyperdexClient = hyperdexClient;
    }

    private String sbToStr(StaticBuffer sb){
        return new String(sb.as(StaticBuffer.ARRAY_FACTORY));
    }

    private ByteString toByteString(StaticBuffer sb) throws UnsupportedEncodingException {
        return ByteString.encode(sbToStr(sb), UTF_8);
    }

    private StaticBuffer decode(ByteString bs) throws UnsupportedEncodingException {
//        return new StaticArrayBuffer(bs.decode(UTF_8).getBytes());
        return new StaticArrayBuffer(bs.getBytes());
    }

    private String hash(byte[] data){
        return DigestUtils.md5Hex(data);
    }

    private ByteString buildVid(StaticBuffer key){
        byte[] a = name.getBytes();
        byte[] b = key.asByteBuffer().array();
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return new ByteString(c);
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws StorageException {
        HashMap<String, Object> data = new HashMap<String, Object>();

        log.trace("insert record");

        try {

            data.put(KEY, new ByteString(key.asByteBuffer().array()));
            data.put(VALUE_KEY, toByteString(value));

            ByteString vid = buildVid(key); //hash(key.as(StaticBuffer.ARRAY_FACTORY));

            this.hyperdexClient.put(SPACE_NAME, vid, data);

        } catch (HyperDexClientException e) {
            throw new PermanentStorageException(e.getMessage());
        }catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new PermanentStorageException(e.getMessage());
        }
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(StaticBuffer keyStart, StaticBuffer keyEnd,
                                                  KeySelector selector, StoreTransaction txh) throws StorageException {

        final List<KeyValueEntry> result = new ArrayList<KeyValueEntry>();
//        final List<Long> resultDebug = new ArrayList<Long>();

        log.trace("getSlice");

        try {

            HashMap<String, Object> predicates = new HashMap<String, Object>();

            if (keyStart.compareTo(keyEnd) > 0)
                return toRecordIterator(result);

            predicates.put(KEY, new Range(new ByteString(keyStart.asByteBuffer().array()),
                    new ByteString(keyEnd.asByteBuffer().array())));

            Iterator hdxRv = this.hyperdexClient.sorted_search(SPACE_NAME, predicates, KEY, 2000000, false);

//            if (hdxRv.hasNext())
//                hdxRv.next(); // remove first record

            boolean shouldRemoveLast;
            StaticBuffer lastValue = ByteBufferUtil.emptyBuffer();

            while(hdxRv.hasNext()){

                if (selector.reachedLimit())
                    break;

                Object o = hdxRv.next();

                Map<String, Object > om = (o instanceof Map ? (Map) o : null);

                if (om == null){
                    throw new PermanentStorageException("record return null");
                }

                lastValue = decode((ByteString) om.get(KEY));

                KeyValueEntry kve = new KeyValueEntry(lastValue,
                        decode((ByteString) om.get(VALUE_KEY)));

                result.add(kve);

                selector.include(lastValue);

            }

            shouldRemoveLast = lastValue.compareTo(keyEnd) >= 0;

            // remove last
            if (result.size() > 0 && shouldRemoveLast)
                result.remove(result.size() -1);

//        } catch (HyperDexClientException e) {
//            e.printStackTrace();
//            throw new PermanentStorageException(e.getMessage());
        } catch (IOException e){
            throw new PermanentStorageException(e.getMessage());
        }

        return toRecordIterator(result);
    }

//    private class KVComparator implements Comparator<KeyValueEntry>{
//        @Override
//        public int compare(KeyValueEntry a, KeyValueEntry b) {
//            return ByteBufferUtil.compare(a.getKey(), b.getKey());
//        }
//    }

    private RecordIterator<KeyValueEntry> toRecordIterator(final List<KeyValueEntry> result) {

//        Collections.sort(result, new KVComparator());

        return new RecordIterator<KeyValueEntry>() {
            private final java.util.Iterator<KeyValueEntry> entries = result.iterator();

            @Override
            public void close() throws IOException {
            }

            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public KeyValueEntry next() {
                return entries.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException {
        try {
            ByteString vid = buildVid(key); //hash(key.as(StaticBuffer.ARRAY_FACTORY));
            this.hyperdexClient.del(SPACE_NAME, vid);
        } catch (HyperDexClientException e) {
            throw new PermanentStorageException(e.getMessage());
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException {

        StaticBuffer rv;

        try {

            ByteString vid = buildVid(key); //hash(key.as(StaticBuffer.ARRAY_FACTORY)); //ByteString.encode(sbToStr(key), StandardCharsets.UTF_8.name());

            Map<String, Object> hdxRv = this.hyperdexClient.get_partial(SPACE_NAME,
                    vid, SELECT_ATTRS);

            if (hdxRv == null)
                return null;

            ByteString value = (ByteString) hdxRv.get(VALUE_KEY);

            rv = new StaticArrayBuffer(value.decode(UTF_8).getBytes());

        } catch (HyperDexClientException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            throw new PermanentStorageException(e.getMessage());
        } catch (UnsupportedEncodingException e){
            e.printStackTrace();
            throw new PermanentStorageException(e.getMessage());
//        } catch (IOException e){
//            e.printStackTrace();
//            throw new PermanentStorageException(e.getMessage());
        }

        return rv;
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return get(key, txh) != null;
    }


    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        // we need no locking
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws StorageException {
        manager.removeDatabase(this);
    }

//    private StaticBuffer objToSb(Object obj) throws IOException {
//        return new StaticArrayBuffer(serialize(obj));
//    }
//
//    private static byte[] serialize(Object obj) throws IOException {
//        ByteArrayOutputStream b = new ByteArrayOutputStream();
//        ObjectOutputStream o = new ObjectOutputStream(b);
//        o.writeObject(obj);
//        return b.toByteArray();
//    }
//
//    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
//        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
//        ObjectInputStream o = new ObjectInputStream(b);
//        return o.readObject();
//    }
}
