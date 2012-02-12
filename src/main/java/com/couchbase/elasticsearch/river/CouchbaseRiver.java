package com.couchbase.elasticsearch.river;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

public class CouchbaseRiver extends AbstractRiverComponent implements River {

    // River fields
    private final Client client;
    private final String riverIndexName;

    // Couchbase fields
    private final List<URI> couchbaseURIs;
    private final String couchbaseBucket;
    private final String couchbaseBucketPassword;

    // Elastic Search index fields
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final int throttleSize;

    // Thrads
    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    // Input/Output Queue
    private final BlockingQueue<String> stream;

    public CouchbaseRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("couchbase")) {
            Map<String, Object> couchbaseSettings = (Map<String, Object>) settings.settings().get("couchbase");
            Object uris = couchbaseSettings.get("uris");
            logger.debug("uris class is {}", uris.getClass());
            logger.debug("uris is {}", uris.toString());
            //fixme use this field to populate this
            couchbaseURIs = Arrays.asList(URI.create("http://loclahost:8091/pools"));

            couchbaseBucket = XContentMapValues.nodeStringValue(couchbaseSettings.get("bucket"), riverName.name());
            couchbaseBucketPassword = XContentMapValues.nodeStringValue(couchbaseSettings.get("bucketPassword"), riverName.name());
        } else {
            //defaults
            couchbaseURIs = Arrays.asList(URI.create("http://loclahost:8091/pools"));
            couchbaseBucket = "default";
            couchbaseBucketPassword = "";
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), couchbaseBucket);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), couchbaseBucket);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            throttleSize = XContentMapValues.nodeIntegerValue(indexSettings.get("throttle_size"), bulkSize * 5);
        } else {
            indexName = couchbaseBucket;
            typeName = couchbaseBucket;
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            throttleSize = bulkSize * 5;
        }
        if (throttleSize == -1) {
            stream = new LinkedTransferQueue<String>();
        } else {
            stream = new ArrayBlockingQueue<String>(throttleSize);
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

}
