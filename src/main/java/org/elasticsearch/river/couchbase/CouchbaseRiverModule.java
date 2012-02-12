package org.elasticsearch.river.couchbase;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class CouchbaseRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(CouchbaseRiver.class).asEagerSingleton();
    }

}
