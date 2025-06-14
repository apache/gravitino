package org.apache.gravitino.catalog;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catalog operation metrics collector
 */
public class CatalogMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogMetrics.class);

    private final AtomicLong classLoaderCreations = new AtomicLong(0);
    private final AtomicLong classLoaderReleases = new AtomicLong(0);
    private final AtomicLong hotUpdates = new AtomicLong(0);
    private final AtomicLong fullUpdates = new AtomicLong(0);
    private final AtomicLong rateLimitHits = new AtomicLong(0);

    public void recordClassLoaderCreation() {
        long count = classLoaderCreations.incrementAndGet();
        if (count % 100 == 0) {
            LOG.info("ClassLoader creations: {}", count);
        }
    }

    public void recordClassLoaderRelease() {
        classLoaderReleases.incrementAndGet();
    }

    public void recordHotUpdate() {
        hotUpdates.incrementAndGet();
    }

    public void recordFullUpdate() {
        fullUpdates.incrementAndGet();
    }

    public void recordRateLimitHit() {
        rateLimitHits.incrementAndGet();
    }

    public void logMetrics() {
        LOG.info("Catalog Metrics - ClassLoader creations: {}, releases: {}, " +
                        "hot updates: {}, full updates: {}, rate limit hits: {}",
                classLoaderCreations.get(), classLoaderReleases.get(),
                hotUpdates.get(), fullUpdates.get(), rateLimitHits.get());
    }
}