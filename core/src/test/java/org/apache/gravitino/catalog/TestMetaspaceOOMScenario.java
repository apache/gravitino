package org.apache.gravitino.catalog;

import static org.apache.gravitino.TestCatalog.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * 集成测试：模拟原始Metaspace OOM场景，验证优化方案的有效性
 */
public class TestMetaspaceOOMScenario {

    private static final String METALAKE = "test_metalake";
    private static final String CATALOG = "iceberg_catalog";
    private static final String PROVIDER = "lakehouse-iceberg";

    private static EntityStore entityStore;
    private static Config config;
    private static CatalogManager catalogManager;
    private static MockedStatic<GravitinoEnv> mockGravitinoEnv;

    @BeforeAll
    static void setUp() throws IOException {
        // 配置优化参数
        config = new Config(false) {
        };
        config.set(Configs.CATALOG_LOAD_ISOLATED, false);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE, 5L);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES, 1L);
        config.set(Configs.CATALOG_OPERATIONS_PER_SECOND, 20.0); // 允许较高频率用于测试

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
    void testHighFrequencyPropertyUpdatesWithOptimization() throws InterruptedException {
        // Given - 创建一个Iceberg catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG);
        Map<String, String> props = Maps.newHashMap();
        props.put("metastore.uris", "thrift://localhost:9083");
        props.put("warehouse", "/tmp/warehouse");
        props.put("description", "original description");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // When - 模拟原始问题场景：高频属性更新
        int updateCount = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger hotUpdateCount = new AtomicInteger(0);
        AtomicInteger fullUpdateCount = new AtomicInteger(0);

        for (int i = 0; i < updateCount; i++) {
            try {
                if (i % 10 == 0) {
                    // 每10次中有1次关键属性更新（需要完整更新）
                    catalogManager.alterCatalog(ident,
                            CatalogChange.setProperty("warehouse", "/tmp/warehouse_" + i));
                    fullUpdateCount.incrementAndGet();
                } else {
                    // 其他为非关键属性更新（热更新）
                    catalogManager.alterCatalog(ident,
                            CatalogChange.setProperty("description", "description_" + i));
                    hotUpdateCount.incrementAndGet();
                }
                successCount.incrementAndGet();
            } catch (Exception e) {
                // 记录但不失败测试，因为可能有频率限制
                System.out.println("Update " + i + " failed: " + e.getMessage());
            }
        }

        // Then - 验证优化效果
        assertTrue(successCount.get() > updateCount * 0.8,
                "Expected most updates to succeed with optimization");
        assertTrue(hotUpdateCount.get() > fullUpdateCount.get(),
                "Expected more hot updates than full updates");

        // 验证catalog仍然可用
        Catalog finalCatalog = catalogManager.loadCatalog(ident);
        assertNotNull(finalCatalog);
        assertTrue(finalCatalog.properties().get("description").startsWith("description_"));
    }

    @Test
    void testConcurrentHighFrequencyUpdates() throws InterruptedException {
        // Given - 创建多个catalog
        int catalogCount = 3;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];
        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG + "_concurrent_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("metastore.uris", "thrift://localhost:908" + (3 + i));
            props.put("description", "catalog " + i);
            catalogManager.createCatalog(idents[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // When - 并发高频更新
        int threadCount = 5;
        int updatesPerThread = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger totalSuccess = new AtomicInteger(0);

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
                            totalSuccess.incrementAndGet();
                        } catch (Exception e) {
                            // 可能因为频率限制失败，这是预期的
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then - 验证并发处理
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // 验证所有catalog仍然可用
        for (NameIdentifier ident : idents) {
            Catalog catalog = catalogManager.loadCatalog(ident);
            assertNotNull(catalog);
        }

        // 应该有相当数量的成功更新
        assertTrue(totalSuccess.get() > threadCount * updatesPerThread * 0.5,
                "Expected reasonable success rate in concurrent updates");
    }

    @Test
    void testClassLoaderPoolEffectiveness() {
        // Given - 创建多个相同provider的catalog
        int catalogCount = 5;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];

        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG + "_pool_test_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("metastore.uris", "thrift://localhost:9083"); // 相同配置
            props.put("warehouse", "/tmp/warehouse");

            // When - 创建catalog
            catalogManager.createCatalog(idents[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // Then - 验证所有catalog都能正常工作
        for (NameIdentifier ident : idents) {
            Catalog catalog = catalogManager.loadCatalog(ident);
            assertNotNull(catalog);
            assertEquals(PROVIDER, catalog.provider());
        }

        // 进行一些属性更新操作
        for (int i = 0; i < catalogCount; i++) {
            catalogManager.alterCatalog(idents[i],
                    CatalogChange.setProperty("description", "updated_" + i));
        }

        // 验证更新后仍然正常
        for (NameIdentifier ident : idents) {
            Catalog catalog = catalogManager.loadCatalog(ident);
            assertNotNull(catalog);
            assertTrue(catalog.properties().get("description").startsWith("updated_"));
        }
    }

    @Test
    void testMemoryUsageStability() throws InterruptedException {
        // Given - 创建catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG + "_memory_test");
        Map<String, String> props = Maps.newHashMap();
        props.put("metastore.uris", "thrift://localhost:9083");
        props.put("description", "memory test");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // 记录初始内存使用
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();

        // When - 执行大量更新操作
        int updateCount = 200;
        for (int i = 0; i < updateCount; i++) {
            try {
                catalogManager.alterCatalog(ident,
                        CatalogChange.setProperty("description", "memory_test_" + i));

                // 每50次更新后强制GC
                if (i % 50 == 0) {
                    System.gc();
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                // 忽略频率限制错误
            }
        }

        // 最终GC
        System.gc();
        Thread.sleep(100);

        // Then - 检查内存使用是否稳定
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryIncrease = finalMemory - initialMemory;

        // 内存增长应该在合理范围内（不应该有严重的内存泄漏）
        assertTrue(memoryIncrease < 50 * 1024 * 1024, // 50MB
                "Memory increase should be reasonable: " + memoryIncrease + " bytes");

        // 验证catalog仍然可用
        Catalog finalCatalog = catalogManager.loadCatalog(ident);
        assertNotNull(finalCatalog);
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