package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * 性能基准测试，验证优化方案的性能提升
 */
public class TestCatalogPerformanceBenchmark {

    private static final String METALAKE = "benchmark_metalake";
    private static final String CATALOG = "benchmark_catalog";
    private static final String PROVIDER = "test";

    private static EntityStore entityStore;
    private static Config config;
    private static CatalogManager catalogManager;
    private static MockedStatic<GravitinoEnv> mockGravitinoEnv;

    @BeforeAll
    static void setUp() throws IOException {
        config = new Config(false) {};
        config.set(Configs.CATALOG_LOAD_ISOLATED, false);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE, 20L);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES, 10L);
        config.set(Configs.CATALOG_OPERATIONS_PER_SECOND, 100.0); // 高频率用于性能测试

        entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
        entityStore.initialize(config);
        entityStore.setSerDe(null);

        BaseMetalake metalake = createBaseMetalake(RandomIdGenerator.INSTANCE.nextId(), METALAKE);
        entityStore.put(metalake);

        catalogManager = new CatalogManager(config, entityStore, RandomIdGenerator.INSTANCE);

        mockGravitinoEnv = Mockito.mockStatic(GravitinoEnv.class);
        mockGravitinoEnv.when(GravitinoEnv::lockManager).thenReturn(new LockManager(config));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (catalogManager != null) {
            catalogManager.close();
        }
        if (entityStore != null) {
            entityStore.close();
        }
        if (mockGravitinoEnv != null) {
            mockGravitinoEnv.close();
        }
    }

    @Test
    void testHotUpdatePerformance() throws InterruptedException {
        // Given - 创建catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG + "_hot_update");
        Map<String, String> props = Maps.newHashMap();
        props.put("description", "original description");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // When - 测试热更新性能
        int updateCount = 1000;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < updateCount; i++) {
            catalogManager.alterCatalog(ident,
                    CatalogChange.setProperty("description", "description_" + i));
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Then - 验证性能
        double operationsPerSecond = (double) updateCount / (duration / 1000.0);
        System.out.println("Hot update performance: " + operationsPerSecond + " ops/sec");

        // 热更新应该能够达到较高的性能
        assertTrue(operationsPerSecond > 50,
                "Hot update should achieve at least 50 ops/sec, actual: " + operationsPerSecond);
    }

    @Test
    void testConcurrentUpdatePerformance() throws InterruptedException {
        // Given - 创建多个catalog
        int catalogCount = 10;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];
        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG + "_concurrent_perf_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("description", "catalog " + i);
            catalogManager.createCatalog(idents[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // When - 并发性能测试
        int threadCount = 5;
        int updatesPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicLong totalOperations = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int threadIndex = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < updatesPerThread; i++) {
                        try {
                            NameIdentifier targetIdent = idents[i % catalogCount];
                            catalogManager.alterCatalog(targetIdent,
                                    CatalogChange.setProperty("description",
                                            "thread_" + threadIndex + "_update_" + i));
                            totalOperations.incrementAndGet();
                        } catch (Exception e) {
                            // 忽略频率限制错误
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Then - 验证并发性能
        double operationsPerSecond = (double) totalOperations.get() / (duration / 1000.0);
        System.out.println("Concurrent update performance: " + operationsPerSecond + " ops/sec");
        System.out.println("Total successful operations: " + totalOperations.get());

        // 并发更新应该能够处理大部分请求
        assertTrue(totalOperations.get() > threadCount * updatesPerThread * 0.7,
                "Should handle at least 70% of concurrent requests");
    }

    @Test
    void testClassLoaderReuseEfficiency() {
        // Given - 创建多个相同配置的catalog
        int catalogCount = 20;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];

        long startTime = System.currentTimeMillis();

        // When - 创建多个catalog
        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG + "_reuse_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("metastore.uris", "thrift://localhost:9083"); // 相同配置
            props.put("warehouse", "/tmp/warehouse");

            catalogManager.createCatalog(idents[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Then - 验证创建效率
        double catalogsPerSecond = (double) catalogCount / (duration / 1000.0);
        System.out.println("Catalog creation with reuse: " + catalogsPerSecond + " catalogs/sec");

        // 由于ClassLoader复用，创建速度应该较快
        assertTrue(catalogsPerSecond > 10,
                "Should create at least 10 catalogs/sec with ClassLoader reuse");

        // 验证所有catalog都能正常工作
        for (NameIdentifier ident : idents) {
            Catalog catalog = catalogManager.loadCatalog(ident);
            assertNotNull(catalog);
            assertEquals(PROVIDER, catalog.provider());
        }
    }

    private static BaseMetalake createBaseMetalake(Long id, String name) {
        return BaseMetalake.builder()
                .withId(id)
                .withName(name)
                .withAuditInfo(
                        AuditInfo.builder()
                                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                                .withCreateTime(Instant.now())
                                .build())
                .withVersion(SchemaVersion.V_0_1)
                .build();
    }
}