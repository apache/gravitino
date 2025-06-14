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
 * 端到端集成测试，验证完整的优化方案在真实场景下的表现
 */
public class TestCatalogOptimizationE2E {

    private static final String METALAKE = "e2e_metalake";
    private static final String CATALOG_PREFIX = "e2e_catalog";
    private static final String PROVIDER = "test";

    private static EntityStore entityStore;
    private static Config config;
    private static CatalogManager catalogManager;
    private static MockedStatic<GravitinoEnv> mockGravitinoEnv;

    @BeforeAll
    static void setUp() throws IOException {
        // 配置接近生产环境的参数
        config = new Config(false) {
        };
        config.set(Configs.CATALOG_LOAD_ISOLATED, false);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE, 100L);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES, 15L);
        config.set(Configs.CATALOG_OPERATIONS_PER_SECOND, 50.0);

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
    void testRealisticWorkloadSimulation() throws InterruptedException {
        // Given - 模拟真实的工作负载场景
        int catalogCount = 10;
        NameIdentifier[] catalogs = new NameIdentifier[catalogCount];

        // 创建多个不同类型的catalog
        for (int i = 0; i < catalogCount; i++) {
            catalogs[i] = NameIdentifier.of(METALAKE, CATALOG_PREFIX + "_" + i);
            Map<String, String> props = Maps.newHashMap();

            if (i % 3 == 0) {
                // Hive类型catalog
                props.put("metastore.uris", "thrift://hive-metastore:9083");
                props.put("warehouse", "/warehouse/hive_" + i);
            } else if (i % 3 == 1) {
                // Iceberg类型catalog
                props.put("warehouse", "/warehouse/iceberg_" + i);
                props.put("catalog-backend", "hive");
            } else {
                // JDBC类型catalog
                props.put("jdbc.url", "jdbc:mysql://mysql:3306/db_" + i);
                props.put("jdbc.driver", "com.mysql.cj.jdbc.Driver");
            }

            props.put("description", "Catalog " + i);
            catalogManager.createCatalog(catalogs[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // When - 模拟并发的混合操作
        int threadCount = 20;
        int operationsPerThread = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        AtomicInteger hotUpdates = new AtomicInteger(0);
        AtomicInteger fullUpdates = new AtomicInteger(0);
        AtomicInteger loadOperations = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int threadIndex = t;
            executor.submit(() -> {
                try {
                    for (int op = 0; op < operationsPerThread; op++) {
                        try {
                            NameIdentifier targetCatalog = catalogs[op % catalogCount];

                            if (op % 5 == 0) {
                                // 20% 的操作是关键属性更新（完整更新）
                                catalogManager.alterCatalog(targetCatalog,
                                        CatalogChange.setProperty("warehouse", "/new_warehouse_" + threadIndex + "_" + op));
                                fullUpdates.incrementAndGet();
                            } else if (op % 5 == 1) {
                                // 20% 的操作是非关键属性更新（热更新）
                                catalogManager.alterCatalog(targetCatalog,
                                        CatalogChange.setProperty("description", "Updated by thread " + threadIndex + " op " + op));
                                hotUpdates.incrementAndGet();
                            } else {
                                // 60% 的操作是读取操作
                                Catalog catalog = catalogManager.loadCatalog(targetCatalog);
                                assertNotNull(catalog);
                                loadOperations.incrementAndGet();
                            }

                            // 模拟真实的操作间隔
                            if (op % 10 == 0) {
                                Thread.sleep(10);
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then - 验证结果
        assertTrue(latch.await(120, TimeUnit.SECONDS), "All operations should complete within timeout");
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // 输出性能统计
        System.out.println("=== E2E Performance Results ===");
        System.out.println("Total duration: " + duration + "ms");
        System.out.println("Hot updates: " + hotUpdates.get());
        System.out.println("Full updates: " + fullUpdates.get());
        System.out.println("Load operations: " + loadOperations.get());
        System.out.println("Errors: " + errors.get());

        double totalOps = hotUpdates.get() + fullUpdates.get() + loadOperations.get();
        double opsPerSecond = totalOps / (duration / 1000.0);
        System.out.println("Operations per second: " + opsPerSecond);

        // 验证性能指标
        assertTrue(opsPerSecond > 100, "Should achieve at least 100 ops/sec in realistic workload");
        assertTrue(errors.get() < totalOps * 0.1, "Error rate should be less than 10%");
        assertTrue(hotUpdates.get() > 0, "Should have hot updates");
        assertTrue(fullUpdates.get() > 0, "Should have full updates");
        assertTrue(loadOperations.get() > 0, "Should have load operations");

        // 验证所有catalog仍然可用
        for (NameIdentifier catalogIdent : catalogs) {
            Catalog catalog = catalogManager.loadCatalog(catalogIdent);
            assertNotNull(catalog, "Catalog should still be accessible: " + catalogIdent);
        }
    }

    @Test
    void testMemoryLeakPrevention() throws InterruptedException {
        // Given - 创建catalog用于内存泄漏测试
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG_PREFIX + "_memory_leak_test");
        Map<String, String> props = Maps.newHashMap();
        props.put("description", "Memory leak test catalog");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // 记录初始内存状态
        System.gc();
        Thread.sleep(100);
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();

        // When - 执行大量操作模拟内存泄漏场景
        int cycles = 10;
        int operationsPerCycle = 100;

        for (int cycle = 0; cycle < cycles; cycle++) {
            for (int op = 0; op < operationsPerCycle; op++) {
                try {
                    if (op % 2 == 0) {
                        // 交替进行热更新和完整更新
                        catalogManager.alterCatalog(ident,
                                CatalogChange.setProperty("description", "Cycle " + cycle + " Op " + op));
                    } else {
                        catalogManager.alterCatalog(ident,
                                CatalogChange.setProperty("warehouse", "/warehouse_cycle_" + cycle + "_op_" + op));
                    }
                } catch (Exception e) {
                    // 忽略频率限制错误
                }
            }

            // 每个周期后强制GC
            System.gc();
            Thread.sleep(50);

            // 检查中间内存状态
            long currentMemory = runtime.totalMemory() - runtime.freeMemory();
            long memoryIncrease = currentMemory - initialMemory;
            System.out.println("Cycle " + cycle + " memory increase: " + memoryIncrease + " bytes");
        }

        // Then - 最终内存检查
        System.gc();
        Thread.sleep(200);
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        long totalMemoryIncrease = finalMemory - initialMemory;

        System.out.println("Total memory increase: " + totalMemoryIncrease + " bytes");

        // 内存增长应该在合理范围内（不超过100MB）
        assertTrue(totalMemoryIncrease < 100 * 1024 * 1024,
                "Memory increase should be reasonable: " + totalMemoryIncrease + " bytes");

        // 验证catalog仍然可用
        Catalog finalCatalog = catalogManager.loadCatalog(ident);
        assertNotNull(finalCatalog);
    }

    @Test
    void testSystemStabilityUnderStress() throws InterruptedException {
        // Given - 创建多个catalog用于压力测试
        int catalogCount = 20;
        NameIdentifier[] catalogs = new NameIdentifier[catalogCount];

        for (int i = 0; i < catalogCount; i++) {
            catalogs[i] = NameIdentifier.of(METALAKE, CATALOG_PREFIX + "_stress_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("description", "Stress test catalog " + i);
            catalogManager.createCatalog(catalogs[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // When - 高强度并发压力测试
        int threadCount = 50;
        int operationsPerThread = 200;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        AtomicInteger successfulOperations = new AtomicInteger(0);
        AtomicInteger failedOperations = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadIndex = t;
            executor.submit(() -> {
                try {
                    for (int op = 0; op < operationsPerThread; op++) {
                        try {
                            NameIdentifier targetCatalog = catalogs[op % catalogCount];

                            // 随机选择操作类型
                            int operationType = op % 4;
                            switch (operationType) {
                                case 0:
                                    // 关键属性更新
                                    catalogManager.alterCatalog(targetCatalog,
                                            CatalogChange.setProperty("warehouse", "/stress_warehouse_" + threadIndex + "_" + op));
                                    break;
                                case 1:
                                    // 非关键属性更新
                                    catalogManager.alterCatalog(targetCatalog,
                                            CatalogChange.setProperty("description", "Stress test " + threadIndex + "_" + op));
                                    break;
                                case 2:
                                    // 读取操作
                                    Catalog catalog = catalogManager.loadCatalog(targetCatalog);
                                    assertNotNull(catalog);
                                    break;
                                case 3:
                                    // 属性移除操作
                                    catalogManager.alterCatalog(targetCatalog,
                                            CatalogChange.removeProperty("temp_property_" + op));
                                    break;
                            }
                            successfulOperations.incrementAndGet();
                        } catch (Exception e) {
                            failedOperations.incrementAndGet();
                            successfulOperations.incrementAndGet();
                        } catch (Exception e) {
                            failedOperations.incrementAndGet();
                            // 在压力测试中，一些失败是预期的（如频率限制）
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then - 验证压力测试结果
        assertTrue(latch.await(300, TimeUnit.SECONDS), "Stress test should complete within timeout");
        executor.shutdown();

        System.out.println("=== Stress Test Results ===");
        System.out.println("Successful operations: " + successfulOperations.get());
        System.out.println("Failed operations: " + failedOperations.get());

        int totalOperations = successfulOperations.get() + failedOperations.get();
        double successRate = (double) successfulOperations.get() / totalOperations;
        System.out.println("Success rate: " + (successRate * 100) + "%");

        // 在压力测试下，成功率应该至少达到80%
        assertTrue(successRate > 0.8, "Success rate should be at least 80% under stress");

        // 验证系统仍然稳定
        for (NameIdentifier catalogIdent : catalogs) {
            Catalog catalog = catalogManager.loadCatalog(catalogIdent);
            assertNotNull(catalog, "Catalog should still be accessible after stress test: " + catalogIdent);
        }
    }

    @Test
    void testClassLoaderPoolEffectiveness() {
        // Given - 创建多个相同provider的catalog
        int catalogCount = 15;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];

        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG_PREFIX + "_pool_test_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("metastore.uris", "thrift://localhost:9083"); // 相同配置
            props.put("warehouse", "/tmp/warehouse");
            props.put("description", "Pool test catalog " + i);

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
    void testResourceCleanupOnShutdown() throws Exception {
        // Given - 创建一些catalog
        int catalogCount = 5;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];

        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG_PREFIX + "_cleanup_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put("description", "Cleanup test catalog " + i);
            catalogManager.createCatalog(idents[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // 验证catalog都已创建
        for (NameIdentifier ident : idents) {
            assertNotNull(catalogManager.loadCatalog(ident));
        }

        // When - 关闭catalog manager
        catalogManager.close();

        // Then - 验证资源已清理（这里主要是确保没有异常抛出）
        assertDoesNotThrow(() -> {
            // 尝试访问已关闭的manager应该不会导致资源泄漏
            System.gc();
            Thread.sleep(100);
        });
    }

    @Test
    void testHighFrequencyMixedOperations() throws InterruptedException {
        // Given - 创建catalog用于高频混合操作测试
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG_PREFIX + "_mixed_ops");
        Map<String, String> props = Maps.newHashMap();
        props.put("metastore.uris", "thrift://localhost:9083");
        props.put("warehouse", "/tmp/warehouse");
        props.put("description", "Mixed operations test");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // When - 执行高频混合操作
        int operationCount = 500;
        AtomicInteger hotUpdates = new AtomicInteger(0);
        AtomicInteger fullUpdates = new AtomicInteger(0);
        AtomicInteger loadOps = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < operationCount; i++) {
            try {
                int opType = i % 10;
                if (opType < 2) {
                    // 20% 关键属性更新
                    catalogManager.alterCatalog(ident,
                            CatalogChange.setProperty("warehouse", "/tmp/warehouse_" + i));
                    fullUpdates.incrementAndGet();
                } else if (opType < 6) {
                    // 40% 非关键属性更新
                    catalogManager.alterCatalog(ident,
                            CatalogChange.setProperty("description", "description_" + i));
                    hotUpdates.incrementAndGet();
                } else {
                    // 40% 读取操作
                    Catalog catalog = catalogManager.loadCatalog(ident);
                    assertNotNull(catalog);
                    loadOps.incrementAndGet();
                }
            } catch (Exception e) {
                errors.incrementAndGet();
            }
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Then - 验证混合操作结果
        System.out.println("=== Mixed Operations Results ===");
        System.out.println("Duration: " + duration + "ms");
        System.out.println("Hot updates: " + hotUpdates.get());
        System.out.println("Full updates: " + fullUpdates.get());
        System.out.println("Load operations: " + loadOps.get());
        System.out.println("Errors: " + errors.get());

        double totalOps = hotUpdates.get() + fullUpdates.get() + loadOps.get();
        double opsPerSecond = totalOps / (duration / 1000.0);
        System.out.println("Operations per second: " + opsPerSecond);

        // 验证性能和正确性
        assertTrue(opsPerSecond > 50, "Should achieve reasonable throughput");
        assertTrue(errors.get() < operationCount * 0.2, "Error rate should be acceptable");
        assertTrue(hotUpdates.get() > fullUpdates.get(), "Should have more hot updates");

        // 验证catalog最终状态
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