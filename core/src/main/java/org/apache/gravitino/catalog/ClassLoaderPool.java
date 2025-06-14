package org.apache.gravitino.catalog;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassLoader池管理器，用于复用IsolatedClassLoader实例，减少Metaspace内存消耗
 */
public class ClassLoaderPool implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderPool.class);

    // ClassLoader缓存key
    private static class ClassLoaderKey {
        private final String provider;
        private final Map<String, String> criticalProperties;

        public ClassLoaderKey(String provider, Map<String, String> conf) {
            this.provider = provider;
            // 只保留影响ClassLoader创建的关键属性
            this.criticalProperties = extractCriticalProperties(conf);
        }

        private Map<String, String> extractCriticalProperties(Map<String, String> conf) {
            Map<String, String> critical = Maps.newHashMap();
            // 只包含影响classloader路径的属性
            conf.entrySet().stream()
                    .filter(entry -> entry.getKey().contains("package-path") ||
                            entry.getKey().contains("conf-path") ||
                            entry.getKey().contains("authorization"))
                    .forEach(entry -> critical.put(entry.getKey(), entry.getValue()));
            return critical;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ClassLoaderKey)) return false;
            ClassLoaderKey that = (ClassLoaderKey) o;
            return provider.equals(that.provider) &&
                    criticalProperties.equals(that.criticalProperties);
        }

        @Override
        public int hashCode() {
            return provider.hashCode() * 31 + criticalProperties.hashCode();
        }
    }

    // ClassLoader包装器，包含引用计数
    private static class ClassLoaderWrapper {
        private final IsolatedClassLoader classLoader;
        private final AtomicInteger refCount;

        public ClassLoaderWrapper(IsolatedClassLoader classLoader) {
            this.classLoader = classLoader;
            this.refCount = new AtomicInteger(1);
        }

        public IsolatedClassLoader acquire() {
            refCount.incrementAndGet();
            return classLoader;
        }

        public boolean release() {
            return refCount.decrementAndGet() <= 0;
        }

        public void close() {
            classLoader.close();
        }
    }

    private final Cache<ClassLoaderKey, ClassLoaderWrapper> classLoaderCache;

    public ClassLoaderPool(long maxSize, long expireAfterAccessMinutes) {
        this.classLoaderCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccessMinutes, TimeUnit.MINUTES)
                .removalListener((RemovalListener<ClassLoaderKey, ClassLoaderWrapper>)
                        (key, value, cause) -> {
                            if (value != null) {
                                LOG.info("Closing cached ClassLoader for provider: {}", key.provider);
                                value.close();
                            }
                        })
                .build();
    }

    /**
     * 获取或创建ClassLoader
     */
    public IsolatedClassLoader getOrCreate(String provider, Map<String, String> conf,
                                           ClassLoaderFactory factory) {
        ClassLoaderKey key = new ClassLoaderKey(provider, conf);
        ClassLoaderWrapper wrapper = classLoaderCache.get(key, k -> {
            LOG.info("Creating new ClassLoader for provider: {}", provider);
            return new ClassLoaderWrapper(factory.create(provider, conf));
        });
        return wrapper.acquire();
    }

    /**
     * 释放ClassLoader引用
     */
    public void release(String provider, Map<String, String> conf) {
        ClassLoaderKey key = new ClassLoaderKey(provider, conf);
        ClassLoaderWrapper wrapper = classLoaderCache.getIfPresent(key);
        if (wrapper != null && wrapper.release()) {
            classLoaderCache.invalidate(key);
        }
    }

    @Override
    public void close() {
        classLoaderCache.invalidateAll();
    }

    @FunctionalInterface
    public interface ClassLoaderFactory {
        IsolatedClassLoader create(String provider, Map<String, String> conf);
    }
}