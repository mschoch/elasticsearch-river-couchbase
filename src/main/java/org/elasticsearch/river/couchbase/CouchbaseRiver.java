/**
 *   Copyright 2012 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.elasticsearch.river.couchbase;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapOpcode;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.TapClient;
import com.couchbase.client.TapConnect;

public class CouchbaseRiver extends AbstractRiverComponent implements River {

    // River fields
    private final Client client;
    private final String riverIndexName;

    // Couchbase fields
    private final List<URI> couchbaseURIs;
    private final String couchbaseBucket;
    private final String couchbaseBucketPassword;
    private final List<Short> couchbaseVBuckets;
    private final boolean couchbaseAutoBackfill;
    private final boolean couchbaseRegisteredTapClient;
    private final boolean couchbaseDeregisterTapOnShutdown;

    // Elastic Search index fields
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final int throttleSize;

    // Threads
    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    // Input/Output Queue between slurper/indexer
    private final BlockingQueue<ResponseMessage> stream;

    private String tapName;
    private Map<Short, Long> ourClosedCheckpoints = new HashMap<Short, Long>();
    private Map<Short, Long> clusterClosedCheckpoints = null;
    private boolean firstLaunch = false;

    @SuppressWarnings("unchecked")
    @Inject
    public CouchbaseRiver(RiverName riverName, RiverSettings settings,
            @RiverIndexName String riverIndexName, Client client,
            ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("couchbase")) {
            Map<String, Object> couchbaseSettings = (Map<String, Object>) settings
                    .settings().get("couchbase");
            List<String> uris = (List<String>) couchbaseSettings.get("uris");
            couchbaseURIs = new ArrayList<URI>();
            for (String uriString : uris) {
                couchbaseURIs.add(URI.create(uriString));
            }

            couchbaseVBuckets = new ArrayList<Short>();
            List<Integer> vbuckets = (List<Integer>) couchbaseSettings
                    .get("vbuckets");
            for (Integer vbucketId : vbuckets) {
                couchbaseVBuckets.add(new Short(vbucketId.shortValue()));
            }

            couchbaseRegisteredTapClient = XContentMapValues
                    .nodeBooleanValue(
                            couchbaseSettings.get("registeredTapClient"),
                            Boolean.FALSE);
            couchbaseDeregisterTapOnShutdown = XContentMapValues
                    .nodeBooleanValue(
                            couchbaseSettings.get("deregisterTapOnShutdown"),
                            Boolean.FALSE);
            couchbaseAutoBackfill = XContentMapValues.nodeBooleanValue(
                    couchbaseSettings.get("autoBackfill"), Boolean.FALSE);

            couchbaseBucket = XContentMapValues.nodeStringValue(
                    couchbaseSettings.get("bucket"), riverName.name());
            couchbaseBucketPassword = XContentMapValues.nodeStringValue(
                    couchbaseSettings.get("bucketPassword"), riverName.name());
        } else {
            // defaults
            couchbaseURIs = Arrays.asList(URI
                    .create("http://loclahost:8091/pools"));
            couchbaseBucket = "default";
            couchbaseBucketPassword = "";
            couchbaseVBuckets = new ArrayList<Short>();
            couchbaseRegisteredTapClient = false;
            couchbaseDeregisterTapOnShutdown = false;
            couchbaseAutoBackfill = false;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings
                    .settings().get("index");
            indexName = XContentMapValues.nodeStringValue(
                    indexSettings.get("index"), couchbaseBucket);
            typeName = XContentMapValues.nodeStringValue(
                    indexSettings.get("type"), couchbaseBucket);
            bulkSize = XContentMapValues.nodeIntegerValue(
                    indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(
                        XContentMapValues.nodeStringValue(
                                indexSettings.get("bulk_timeout"), "10ms"),
                        TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            throttleSize = XContentMapValues.nodeIntegerValue(
                    indexSettings.get("throttle_size"), bulkSize * 5);
        } else {
            indexName = couchbaseBucket;
            typeName = couchbaseBucket;
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            throttleSize = bulkSize * 5;
        }
        if (throttleSize == -1) {
            stream = new LinkedTransferQueue<ResponseMessage>();
        } else {
            stream = new ArrayBlockingQueue<ResponseMessage>(throttleSize);
        }

    }

    @Override
    public void start() {
        logger.info(
                "Starting couchbase stream: uris [{}], bucket [{}], indexing to [{}]/[{}]",
                couchbaseURIs, couchbaseBucket, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute()
                    .actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we
                // recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event
                // listener here, and only start sampling when the block is
                // removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...",
                        e, indexName);
                return;
            }
        }

        try {
            lookupSavedState();
        } catch (Exception e) {
            logger.warn(
                    "failed to lookup our saved state, disabling river....", e);
            return;
        }

        slurperThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(), "couchbase_river_slurper")
                .newThread(new Slurper());
        slurperThread.start();

        indexerThread = EsExecutors.daemonThreadFactory(
                settings.globalSettings(), "couchbase_river_indexer")
                .newThread(new Indexer());
        indexerThread.start();

    }

    private void lookupSavedState() throws IOException {
        CouchbaseClient cc = new CouchbaseClient(couchbaseURIs,
                couchbaseBucket, couchbaseBucketPassword);

      // find out how many vBuckets there are
      int numVBuckets = cc.getNumVBuckets();
      logger.info("Cluster has {} vbuckets", numVBuckets);

      // make sure we have a list of vbuckets to work with
      if (couchbaseVBuckets.isEmpty()) {
          // configuration did not specify a list of vbuckets to operate on
          // assume we operate on all of them
          for (short i = 0; i < numVBuckets; i++) {
              couchbaseVBuckets.add(i);
          }
          logger.debug("Couchbase vbuckets is: {}", couchbaseVBuckets);
      }

        // refresh the river index so we can query it for our saved state
        client.admin().indices().prepareRefresh(riverIndexName).execute()
                .actionGet();

        // lookup the name we used for our tap stream
        lookupTapName();
        if (tapName == null) {
            // no tap name in our saved state, create a new one and save it
            firstLaunch = true;
            generateTapNameAndSave();
            logger.info("Detected first launch, generated unique tap name {}", tapName);
        }

        lookupClosedCheckpoints(cc);
    }

    private void lookupClosedCheckpoints(CouchbaseClient cc) {
        // now we need to discover the last closed checkpoints for the vbuckets
        // we're working with
        Map<SocketAddress, Map<String, String>> checkpointStats = cc
                .getStats("checkpoint");
        logger.debug("Checkpoint stats are: {}", checkpointStats);
        clusterClosedCheckpoints = discoverVbucketClosedCheckpointIds(
                checkpointStats, couchbaseVBuckets);
    }

    private void generateTapNameAndSave() throws IOException {
        tapName = "elastic-search-" + UUID.randomUUID().toString();
        IndexRequest saveTapNameIndexRequest = indexRequest(riverIndexName)
                .type(riverName.name())
                .id("tap")
                .source(jsonBuilder().startObject().field("name", tapName)
                        .endObject());
        client.index(saveTapNameIndexRequest).actionGet();
    }

    private void lookupTapName() {
        GetResponse tapNameResponse = client
                .prepareGet(riverIndexName, riverName().name(), "tap")
                .execute().actionGet();
        logger.debug("looking up {}/{}/{}", riverIndexName, riverName().name(), "tap");
        if (tapNameResponse.isExists()) {
            logger.debug("it exists");
            tapName = (String) tapNameResponse.sourceAsMap().get("name");
        } else {
            logger.debug("does not exist");
        }
    }

    protected Map<Short, Long> discoverVbucketClosedCheckpointIds(
            Map<SocketAddress, Map<String, String>> checkpointStats,
            List<Short> vbucketIds) {
        Map<Short, Long> result = new HashMap<Short, Long>();
        for (Map<String, String> serverStats : checkpointStats.values()) {
            for (String key : serverStats.keySet()) {
                if (key.startsWith("vb_")
                        && key.contains(":last_closed_checkpoint")) {
                    int colpos = key.indexOf(":");
                    String idString = key.substring(3, colpos);
                    Short id = Short.parseShort(idString);
                    if (vbucketIds.contains(id)) {
                        result.put(Short.parseShort(idString), new Long(
                                serverStats.get(key)));
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        logger.info("closing couchbase stream river");
        slurperThread.interrupt();
        indexerThread.interrupt();

        closed = true;
    }

    private void processMessage(ResponseMessage rm, BulkRequestBuilder bulk) {

        TapOpcode toc = rm.getOpcode();

        if (toc.equals(TapOpcode.START_CHECKPOINT)) {
            // we don't really do anything with these, other than optionally log
            // it
            short vbucket = rm.getVbucket();
            long checkpoint = rm.getCheckpoint();
            logger.debug("See start checkpoint for vbucket {} checkpoint {}",
                    vbucket, checkpoint);
        } else if (toc.equals(TapOpcode.END_CHECKPOINT)) {
            short vbucket = rm.getVbucket();
            long checkpoint = rm.getCheckpoint();
            logger.debug("See end checkpoint for vbucket {} checkpoint {}",
                    vbucket, checkpoint);

            ourClosedCheckpoints.put(vbucket, new Long(checkpoint));

            // build up a document containing our closed checkpoints
            XContentBuilder checkpointBuilder = null;
            try {
                checkpointBuilder = jsonBuilder().startObject();
                checkpointBuilder.field("checkpoint", checkpoint);
                checkpointBuilder.endObject();
            } catch (IOException e) {
                logger.error("Unable to build closed checkpoint document");
                return;
            }

            // index the updated checkpoint map
            bulk.add(indexRequest(riverIndexName).type(riverName.name())
                    .id(String.valueOf(vbucket)).source(checkpointBuilder));

        } else if (toc.equals(TapOpcode.OPAQUE)) {
            logger.debug("see an opaque message");
            // the start of backfills is handled earlier, but we must handle
            // backfill ends here
            int state = rm.getVBucketState();
            if (state == 8) {
                short vbucket = rm.getVbucket();
                logger.debug("Backfill ended for vbucket {}", vbucket);
                // treat this like a closed checkpoint
                Long checkpoint = clusterClosedCheckpoints.remove(vbucket);
                if (checkpoint == null) {
                    logger.debug("cluster checkpoint null for {}", vbucket);
                    return;
                }
                ourClosedCheckpoints.put(vbucket, checkpoint);

                // build up a document containing our closed checkpoints
                XContentBuilder checkpointBuilder = null;
                try {
                    checkpointBuilder = jsonBuilder().startObject();
                    checkpointBuilder.field("checkpoint", checkpoint);
                    checkpointBuilder.endObject();
                } catch (IOException e) {
                    logger.error("Unable to build closed checkpoint document");
                    return;
                }

                // index the updated checkpoint map
                bulk.add(indexRequest(riverIndexName).type(riverName.name())
                        .id(String.valueOf(vbucket)).source(checkpointBuilder));

            }
        } else {

            String key = rm.getKey();
            byte[] value = rm.getValue();
            String rev = fromLong(rm.getCas());

            String prependString = String.format("{\"_rev\":\"%s\",\"_vbucket\":%d,", rev, rm.getVbucket());
            byte[] docBytes = new byte[prependString.length() + value.length - 1];

            System.arraycopy(prependString.getBytes(), 0, docBytes, 0, prependString.length());
            System.arraycopy(value, 1, docBytes, prependString.length(), value.length - 1);

            if (logger.isTraceEnabled()) {
                logger.trace(
                        "processing [index ]: [{}]/[{}]/[{}], source {}",
                        indexName, typeName, key, new String(docBytes));
            }

            IndexRequest indexRequest = indexRequest(indexName).type(typeName)
                    .id(key).source(docBytes)
                    .routing(null)
                    .parent(null);

            bulk.add(indexRequest);

        }

    }

    private class Indexer implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }
                ResponseMessage s;
                try {
                    s = stream.take();
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                    continue;
                }
                BulkRequestBuilder bulk = client.prepareBulk();
                processMessage(s, bulk);

                // spin a bit to see if we can get some more changes
                try {
                    while ((s = stream.poll(bulkTimeout.millis(),
                            TimeUnit.MILLISECONDS)) != null) {
                        processMessage(s, bulk);

                        if (bulk.numberOfActions() >= bulkSize) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

                try {
                    if (bulk.numberOfActions() > 0) {
                        BulkResponse response = bulk.execute().actionGet();
                        if (response.hasFailures()) {
                            // TODO write to exception queue?
                            logger.warn("failed to execute"
                                    + response.buildFailureMessage());
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }
            }
        }
    }

    private class Slurper implements Runnable {

        @Override
        public void run() {

            while (true) {
                if (closed) {
                    return;
                }

                logger.info("Couchbase River Starting");

                TapClient tc = new TapClient(couchbaseURIs, couchbaseBucket, couchbaseBucketPassword);
                try {

                    CouchbaseTapCheckpointManager ctcm = new CouchbaseTapCheckpointManager(riverIndexName, riverName.getName(), client);

                    TapConnect connect = new TapConnect.Builder().backfill().id(tapName).checkpointManager(ctcm).registered(couchbaseRegisteredTapClient).build();

                    tc.tap(connect);

                    while (tc.hasMoreMessages()) {
                        if (closed) {
                            return;
                        }
                        ResponseMessage rm = tc.getNextMessage(60,
                                TimeUnit.SECONDS);
                        if (rm != null) {
                            // check to see if this is the start of a backfill
                            if (isStartOfBackfill(rm)) {
                                // backfill started, check to see if allowed
                                if (!firstLaunch && !couchbaseAutoBackfill) {
                                    logger.error("Backfill required, set autoBackfill=true to allow.  Exiting.");
                                    return;
                                }
                                logger.info("Backfill starting for vbucket {}",
                                        rm.getVbucket());
                            }

                            stream.put(rm);
                        }
                    }
                } catch (Exception e) {
                    tc.shutdown();
                    if (closed) {
                        return;
                    }
                    logger.warn(
                            "failed to read from tap stream, throttling....", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                } finally {
                    if (couchbaseDeregisterTapOnShutdown) {
                        try {
                            tc.tapDeregister(tapName);
                        } catch (ConfigurationException e) {
                            logger.error("Unable to deregister tap stream {}",
                                    tapName, e);
                        } catch (IOException e) {
                            logger.error("Unable to deregister tap stream {}",
                                    tapName, e);
                        }
                    }
                    tc.shutdown();
                }
            }
        }

        /**
         * Check to see if this message indicates the start of a backfill
         *
         * @param rm
         *            the TAP response message to check
         * @return
         */
        private boolean isStartOfBackfill(ResponseMessage rm) {
            if (rm.getOpcode() == TapOpcode.OPAQUE && rm.getVBucketState() == 1) {
                return true;
            }
            return false;
        }
    }

    private static String fromLong(long value) {
        char[] HEX = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
                'b', 'c', 'd', 'e', 'f' };
        char[] hexs;
        int i;
        int c;

        hexs = new char[16];
        for (i = 0; i < 16; i++) {
            c = (int) (value & 0xf);
            hexs[16 - i - 1] = HEX[c];
            value = value >> 4;
        }
        return new String(hexs);
    }

}
