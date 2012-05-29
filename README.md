Couchbase River Plugin for ElasticSearch
========================================

This module is experimental.  While attempts are made to track changes to the bucket map and reconnect accordingly, race-conditions are still present which may result in the loss of data during topology changes.  It should **NOT** be used in production.

In order to install the plugin, simply run: `bin/plugin -install mschoch/elasticsearch-river-couchbase/1.0.3-SNAPSHOT`.

The Couchbase River allows to automatically index Couchbase and make it searchable using the TAP (http://www.couchbase.com/wiki/display/couchbase/TAP+Protocol) stream Couchbase provides. Setting it up is as simple as executing the following against elasticsearch:

    curl -XPUT 'localhost:9200/_river/my_db/_meta' -d '{
        "type" : "couchbase",
        "couchbase" : {
            "uris": ["http://192.168.1.9:8091/pools"],
            "bucket": "default",
            "bucketPassword": ""
        },
        "index" : {
            "index" : "my_db",
            "type" : "my_db",
            "bulk_size" : "100",
            "bulk_timeout" : "10ms"
        }
    }'

This call will create a river that uses the **TAP** stream to index all data within Couchbase. Moreover, any "future" changes will automatically be indexed as well, making your search index and Couchbase synchronized at all times.

The Couchbase river is provided as a [plugin](https://github.com/mschoch/elasticsearch-river-couchbase) (including explanation on how to install it).

On top of that, in case of a failover, the Couchbase river will automatically be started on another elasticsearch node, and continue indexing from the last indexed ts.

Bulking
=======

Bulking is automatically done in order to speed up the indexing process. If within the specified **bulk_timeout** more changes are detected, changes will be bulked up to **bulk_size** before they are indexed.

Building
========

This module is built using maven.  It depends on specific forks of two libraries:

- couchbase-client-1.1-dp-mschoch.jar (source available https://github.com/mschoch/couchbase-java-client )
- spymemcached-2.8.2-SNAPSHOT-mschoch.jar (source available: https://github.com/mschoch/spymemcached/ )

Eventually we hope to merge these changes (or more polished versions of the same) into the mainline Couchbase client.
