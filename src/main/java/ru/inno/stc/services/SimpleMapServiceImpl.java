package ru.inno.stc.services;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMapServiceImpl<K, V> implements SimpleMapService<K, V>, Service {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMapServiceImpl.class);

    @IgniteInstanceResource
    private Ignite            ignite;
    private IgniteCache<K, V> cache;

    @Override
    public void cancel(ServiceContext serviceContext) {
        ignite.destroyCache(serviceContext.name());
    }

    @Override
    public void init(ServiceContext serviceContext) {
        cache = ignite.getOrCreateCache(new CacheConfiguration<>(serviceContext.name()));
        logger.info("Service was initialized: {}", serviceContext.name());
    }

    @Override
    public void execute(ServiceContext serviceContext) {
        logger.info("Distributed execution: {}", serviceContext.name());
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public int size() {
        return cache.size();
    }
}
