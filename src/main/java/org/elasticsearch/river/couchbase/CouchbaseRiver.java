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

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapOpcode;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import com.couchbase.client.TapClient;

public class CouchbaseRiver extends AbstractRiverComponent implements River {

    // River fields
    private final Client client;
    private final String riverIndexName;
    private final String uuid;

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

    // Script
    private final ExecutableScript script;

    // Thrads
    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    // Input/Output Queue
    private final BlockingQueue<Map<String,Object>> stream;

    @Inject
    public CouchbaseRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;
        // generate a unique id for this instance of the river
        this.uuid = UUID.randomUUID().toString();

        if (settings.settings().containsKey("couchbase")) {
            Map<String, Object> couchbaseSettings = (Map<String, Object>) settings.settings().get("couchbase");
            List<String> uris = (List<String>)couchbaseSettings.get("uris");
            couchbaseURIs = new ArrayList<URI>();
            for (String uriString : uris) {
                couchbaseURIs.add(URI.create(uriString));
            }

            couchbaseBucket = XContentMapValues.nodeStringValue(couchbaseSettings.get("bucket"), riverName.name());
            couchbaseBucketPassword = XContentMapValues.nodeStringValue(couchbaseSettings.get("bucketPassword"), riverName.name());

            if (couchbaseSettings.containsKey("script")) {
                script = scriptService.executable("js", couchbaseSettings.get("script").toString(), Maps.newHashMap());
            } else {
                script = null;
            }
        } else {
            //defaults
            couchbaseURIs = Arrays.asList(URI.create("http://loclahost:8091/pools"));
            couchbaseBucket = "default";
            couchbaseBucketPassword = "";
            script = null;
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
            stream = new LinkedTransferQueue<Map<String,Object>>();
        } else {
            stream = new ArrayBlockingQueue<Map<String,Object>>(throttleSize);
        }
    }

    @Override
    public void start() {
        logger.info("starting couchbase stream: uris [{}], bucket [{}], indexing to [{}]/[{}]", couchbaseURIs, couchbaseBucket, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchbase_river_slurper").newThread(new Slurper());
        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchbase_river_indexer").newThread(new Indexer());
        indexerThread.start();
        slurperThread.start();
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

    private Long processLine(Map<String,Object> wrapper, BulkRequestBuilder bulk) {
        long ts = (Long)wrapper.get("ts");
        String key = (String)wrapper.get("key");
        byte[] value = (byte[])wrapper.get("value");

        Map<String, Object> parsedValue;
        try {
            parsedValue = XContentFactory.xContent(XContentType.JSON).createParser(value).mapAndClose();
        } catch (IOException e) {
            logger.warn("failed to parse {}", e, value);
            return null;
        }

        if (parsedValue.containsKey("error")) {
            logger.warn("received error {}", value);
            return null;
        } else {
            //replace the unparsed version with the parsed version
            wrapper.put("value", parsedValue);
        }

        if (script != null) {
            script.setNextVar("ctx", wrapper);
            try {
                logger.debug("before script: {}", wrapper);
                script.run();
                // we need to unwrap the ctx...
                wrapper = (Map<String, Object>) script.unwrap(wrapper);
                logger.debug("after script: {}", wrapper);
            } catch (Exception e) {
                logger.warn("failed to script process {}, ignoring", e, wrapper);
                return ts;
            }
        } else {
            logger.debug("no script");
        }


        if (wrapper.containsKey("ignore") && wrapper.get("ignore").equals(Boolean.TRUE)) {
            // ignore dock
        } else if (wrapper.containsKey("deleted") && wrapper.get("deleted").equals(Boolean.TRUE)) {
            String index = extractIndex(wrapper);
            String type = extractType(wrapper);
            if (logger.isDebugEnabled()) {
                logger.debug("processing [delete]: [{}]/[{}]/[{}]", index, type, key);
            }
            bulk.add(deleteRequest(index).type(type).id(key).routing(extractRouting(wrapper)).parent(extractParent(wrapper)));
        } else {

            String index = extractIndex(parsedValue);
            String type = extractType(parsedValue);
            Map<String, Object> doc = (Map<String, Object>) wrapper.get("value");
            logger.debug("doc to index is: {}", doc);

            if (logger.isDebugEnabled()) {
                logger.debug("processing [index ]: [{}]/[{}]/[{}], source {}", index, type, key, doc);
            }

            bulk.add(indexRequest(index).type(type).id(key).source(doc).routing(extractRouting(parsedValue)).parent(extractParent(parsedValue)));
        }

        return ts;
    }

    private String extractParent(Map<String, Object> wrapper) {
        return (String) wrapper.get("_parent");
    }

    private String extractRouting(Map<String, Object> wrapper) {
        return (String) wrapper.get("_routing");
    }

    private String extractType(Map<String, Object> wrapper) {
        String type = (String) wrapper.get("_type");
        if (type == null) {
            type = typeName;
        }
        return type;
    }

    private String extractIndex(Map<String, Object> wrapper) {
        String index = (String) wrapper.get("_index");
        if (index == null) {
            index = indexName;
        }
        return index;
    }

    private class Indexer implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }
                Map<String,Object> s;
                try {
                   s = stream.take();
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                    continue;
                }
                BulkRequestBuilder bulk = client.prepareBulk();
                Long lastTs = null;
                Long lineTs = processLine(s, bulk);
                if (lineTs != null) {
                    lastTs = lineTs;
                }

                // spin a bit to see if we can get some more changes
                try {
                    while ((s = stream.poll(bulkTimeout.millis(), TimeUnit.MILLISECONDS)) != null) {
                        lineTs = processLine(s, bulk);
                        if (lineTs != null) {
                            lastTs = lineTs;
                        }

                        if (bulk.numberOfActions() >= bulkSize) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

                if (lastTs != null) {
                    try {

                        if (logger.isDebugEnabled()) {
                            logger.debug("processing [_seq  ]: [{}]/[{}]/[{}], last_ts [{}], backlog: {}", riverIndexName, riverName.name(), "_seq", lastTs, stream.size());
                        }
                        bulk.add(indexRequest(riverIndexName).type(riverName.name()).id("_state")
                                .source(jsonBuilder().startObject().startObject("couchbase").field("last_ts", lastTs).endObject().endObject()));
                    } catch (IOException e) {
                        logger.warn("failed to add last_seq entry to bulk indexing");
                    }
                }

                try {
                    BulkResponse response = bulk.execute().actionGet();
                    if (response.hasFailures()) {
                        // TODO write to exception queue?
                        logger.warn("failed to execute" + response.buildFailureMessage());
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

                long lastTs = 0;
                try {
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    GetResponse lastSeqGetResponse = client.prepareGet(riverIndexName, riverName().name(), "_state").execute().actionGet();
                    if (lastSeqGetResponse.exists()) {
                        Map<String, Object> couchdbState = (Map<String, Object>) lastSeqGetResponse.sourceAsMap().get("couchbase");
                        if (couchdbState != null) {
                            lastTs = (Long)couchdbState.get("last_ts");
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to get last_seq, throttling....", e);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("using uris [{}], bucket [{}], ts [{}]", couchbaseURIs, couchbaseBucket, lastTs);
                }

                TapClient tc = new TapClient(couchbaseURIs, couchbaseBucket, couchbaseBucketPassword);

                try {

                    tc.tapBackfill("es-river-" + riverIndexName + "-" + uuid, lastTs, 0, TimeUnit.SECONDS);

                    while(tc.hasMoreMessages()) {
                        if (closed) {
                            return;
                        }
                        ResponseMessage rm = tc.getNextMessage(60, TimeUnit.SECONDS);
                        if(rm != null) {
                            TapOpcode operation = rm.getOpcode();
                            String key = rm.getKey();
                            byte[] value = rm.getValue();

                            if (logger.isTraceEnabled()) {
                                logger.trace("[couchbase] tap message key: {} value: {}", key, new String(value));
                            }

                            // we put here, so we block if there is no space to add
                            Map<String,Object> wrapper = new HashMap<String,Object>();
                            wrapper.put("key", key);
                            wrapper.put("value", value);
                            wrapper.put("ts", (new Date()).getTime());
                            if(operation == TapOpcode.DELETE) {
                                wrapper.put("deleted", true);
                            }
                            stream.put(wrapper);
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("[couchbase] alive, got ACK or no data for 60 seconds, backlog: {}", stream.size());
                            }
                        }
                    }
                } catch(Exception e) {
                    tc.shutdown();
                    if (closed) {
                        return;
                    }
                    logger.warn("failed to read from tap stream, throttling....", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                } finally {
                    tc.shutdown();
                }
            }
        }
    }

}
