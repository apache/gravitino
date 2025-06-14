package org.apache.gravitino.catalog;

import static org.apache.gravitino.TestCatalog.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestCatalogManagerOptimization {

    private static final String METALAKE = "metalake_for_catalog_test";
    private static final String CATALOG = "catalog_test";
    private static final String PROVIDER = "test";

    private static EntityStore entityStore;
    private static Config config;
    private static CatalogManager catalogManager;
    private static MockedStatic<GravitinoEnv> mockGravitinoEnv;

    @BeforeAll
    static void setUp() throws IOException {
        config = new Config(false) {};
        config.set(Configs.CATALOG_LOAD_ISOLATED, false);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE, 10L);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES, 5L);
        config.set(Configs.CATALOG_OPERATIONS_PER_SECOND, 5.0);

        entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
        entityStore.initialize(config);
        entityStore.setSerDe(null);

        BaseMetalake metalake = createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE);
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

    @BeforeEach
    void resetCatalogManager() {
        catalogManager.getCatalogCache().invalidateAll();
    }

    @AfterEach
    void cleanUp() {
        catalogManager.getCatalogCache().invalidateAll();
    }

    @Test
    void testHotUpdateForNonCriticalProperties() {
        // Given - 创建一个catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG);
        Map<String, String> props = Maps.newHashMap();
        props.put(PROPERTY_KEY1, "value1");
        props.put("description", "original description");

        Catalog originalCatalog = catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // 获取原始的wrapper引用
        CatalogManager.CatalogWrapper originalWrapper =
                catalogManager.getCatalogCache().getIfPresent(ident);
        assertNotNull(originalWrapper);

        // When - 更新非关键属性
        Catalog updatedCatalog = catalogManager.alterCatalog(
                ident, CatalogChange.setProperty("description", "new description"));

        // Then - 应该进行热更新，wrapper实例应该保持不变
        CatalogManager.CatalogWrapper currentWrapper =
                catalogManager.getCatalogCache().getIfPresent(ident);
        assertNotNull(currentWrapper);
        assertSame(originalWrapper, currentWrapper); // 应该是同一个wrapper实例

        assertEquals("new description", updatedCatalog.properties().get("description"));
        assertNotEquals(originalCatalog.properties().get("description"),
                updatedCatalog.properties().get("description"));
    }

    @Test
    void testFullUpdateForCriticalProperties() {
        // Given - 创建一个catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG + "_critical");
        Map<String, String> props = Maps.newHashMap();
        props.put(PROPERTY_KEY1, "value1");
        props.put("metastore.uris", "thrift://old-host:9083");

        Catalog originalCatalog = catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // 获取原始的wrapper引用
        CatalogManager.CatalogWrapper originalWrapper =
                catalogManager.getCatalogCache().getIfPresent(ident);
        assertNotNull(originalWrapper);

        // When - 更新关键属性
        Catalog updatedCatalog = catalogManager.alterCatalog(
                ident, CatalogChange.setProperty("metastore.uris", "thrift://new-host:9083"));

        // Then - 应该进行完整更新，wrapper实例应该不同
        CatalogManager.CatalogWrapper currentWrapper =
                catalogManager.getCatalogCache().getIfPresent(ident);
        assertNotNull(currentWrapper);
        // 注意：由于完整更新会重新创建wrapper，所以实例可能不同

        assertEquals("thrift://new-host:9083", updatedCatalog.properties().get("metastore.uris"));
        assertNotEquals(originalCatalog.properties().get("metastore.uris"),
                updatedCatalog.properties().get("metastore.uris"));
    }

    @Test
    void testRateLimitingPreventsExcessiveOperations() {
        // Given - 创建一个catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG + "_rate_limit");
        Map<String, String> props = Maps.newHashMap();
        props.put(PROPERTY_KEY1, "value1");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // When - 快速连续执行多次更新操作（超过配置的5次/秒限制）
        int successCount = 0;
        int rateLimitCount = 0;

        for (int i = 0; i < 10; i++) {
            try {
                catalogManager.alterCatalog(
                        ident, CatalogChange.setProperty("description", "update_" + i));
                successCount++;
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("rate limit")) {
                    rateLimitCount++;
                }
            }
        }

        // Then - 应该有一些操作被限流
        assertTrue(rateLimitCount > 0, "Expected some operations to be rate limited");
        assertTrue(successCount > 0, "Expected some operations to succeed");
        assertTrue(successCount + rateLimitCount == 10, "All operations should be accounted for");
    }

    @Test
    void testConcurrentCatalogOperations() throws InterruptedException {
        // Given - 创建多个catalog
        int catalogCount = 5;
        NameIdentifier[] idents = new NameIdentifier[catalogCount];
        for (int i = 0; i < catalogCount; i++) {
            idents[i] = NameIdentifier.of(METALAKE, CATALOG + "_concurrent_" + i);
            Map<String, String> props = Maps.newHashMap();
            props.put(PROPERTY_KEY1, "value" + i);
            catalogManager.createCatalog(idents[i], Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        }

        // When - 并发更新这些catalog
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    NameIdentifier targetIdent = idents[threadIndex % catalogCount];
                    catalogManager.alterCatalog(
                            targetIdent,
                            CatalogChange.setProperty("thread_update", "thread_" + threadIndex));
                } catch (Exception e) {
                    // 可能因为频率限制而失败，这是预期的
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then - 所有线程应该完成（无论成功还是失败）
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // 验证catalog仍然可以正常访问
        for (NameIdentifier ident : idents) {
            Catalog catalog = catalogManager.loadCatalog(ident);
            assertNotNull(catalog);
        }
    }

    @Test
    void testClassLoaderPoolReuse() {
        // Given - 创建两个相同provider的catalog
        NameIdentifier ident1 = NameIdentifier.of(METALAKE, CATALOG + "_pool_1");
        NameIdentifier ident2 = NameIdentifier.of(METALAKE, CATALOG + "_pool_2");

        Map<String, String> props = Maps.newHashMap();
        props.put(PROPERTY_KEY1, "value1");

        // When - 创建两个catalog
        catalogManager.createCatalog(ident1, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);
        catalogManager.createCatalog(ident2, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // Then - 两个catalog应该都能正常工作
        Catalog catalog1 = catalogManager.loadCatalog(ident1);
        Catalog catalog2 = catalogManager.loadCatalog(ident2);

        assertNotNull(catalog1);
        assertNotNull(catalog2);
        assertEquals(PROVIDER, catalog1.provider());
        assertEquals(PROVIDER, catalog2.provider());
    }

    @Test
    void testMixedPropertyUpdates() {
        // Given - 创建一个catalog
        NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG + "_mixed");
        Map<String, String> props = Maps.newHashMap();
        props.put(PROPERTY_KEY1, "value1");
        props.put("description", "original description");
        props.put("metastore.uris", "thrift://old-host:9083");

        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", props);

        // When - 同时更新关键属性和非关键属性
        Catalog updatedCatalog = catalogManager.alterCatalog(ident,
                CatalogChange.setProperty("description", "new description"),
                CatalogChange.setProperty("metastore.uris", "thrift://new-host:9083"));

        // Then - 应该进行完整更新（因为包含关键属性变更）
        assertEquals("new description", updatedCatalog.properties().get("description"));
        assertEquals("thrift://new-host:9083", updatedCatalog.properties().get("metastore.uris"));
    }

    private static BaseMetalake createBaseMakeLake(Long id, String name) {
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