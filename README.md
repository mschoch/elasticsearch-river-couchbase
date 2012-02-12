Couchbase River Plugin for ElasticSearch
==================================

This module is experimental.  Currently the mechanism to track and resume progress is unreliable.  Future version will do proper checkpointing.  It should **NOT** be used in production.

In order to install the plugin, simply run: `bin/plugin -install mschoch/elasticsearch-river-couchbase/1.0.0-SNAPSHOT`.

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
======

Bulking is automatically done in order to speed up the indexing process. If within the specified **bulk_timeout** more changes are detected, changes will be bulked up to **bulk_size** before they are indexed.

Script Filters
=========

Filtering can be performed by providing a script (default to JavaScript) that will further process each changed item within the TAP stream. The json provided to the script is under a var called **ctx** with the relevant seq stream change (for example, **ctx.key** will refer to the key, **ctx.value** will refer to the value, or **ctx.deleted** is the flag if its deleted or not).

Note, this feature requires the `lang-javascript` plugin.

The **ctx.value** can be changed and its value can will be indexed (assuming its not a deleted change). Also, if **ctx.ignore** is set to true, the change will be ignore and not applied.

Other possible values that can be set are **ctx.index** to control the index name to index the doc into, **ctx.type** to control the (mapping) type to index into, **ctx._parent** and **ctx._routing**.

Here is an example setting that adds `field1` with value 7 to all docs:

    {
        "type" : "couchbase",
        "couchbase" : {
            "script" : "ctx.doc.field1 = 7"
        }
    }