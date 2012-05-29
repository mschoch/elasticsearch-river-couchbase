package org.elasticsearch.river.couchbase;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.couchbase.client.TapCheckpointManager;

public class CouchbaseTapCheckpointManager implements TapCheckpointManager {

    private Client client;
    private String riverIndexName;
    private String riverName;
    private ESLogger logger;

    public CouchbaseTapCheckpointManager(String riverIndexName, String riverName, Client client) {
        this.riverIndexName = riverIndexName;
        this.riverName = riverName;
        this.client = client;
        this.logger = Loggers.getLogger(getClass());
    }

    @Override
    public long getLastClosedCheckpoint(int vbucket) {
        long result = 0;

        GetResponse closedCheckpointResponse = client
                .prepareGet(riverIndexName, riverName, String.valueOf(vbucket))
                .execute().actionGet();

        logger.debug("looking up {}/{}/{}", riverIndexName, riverName, String.valueOf(vbucket));

        if (closedCheckpointResponse.isExists()) {
            Number checkpoint = (Number) closedCheckpointResponse.sourceAsMap().get("checkpoint");
            result = checkpoint.longValue();
        } else {
            logger.debug("No record of closed checkpoint for vbucket {}", vbucket);
        }

        return result;
    }

}
