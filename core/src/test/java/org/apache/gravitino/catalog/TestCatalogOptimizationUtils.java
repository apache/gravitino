package org.apache.gravitino.catalog;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Catalog优化测试工具类
 */
public class TestCatalogOptimizationUtils {

    public static final String TEST_METALAKE = "test_metalake";
    public static final String TEST_CATALOG_PREFIX = "test_catalog";
    public static final String TEST_PROVIDER = "test";

    /**
     * 创建测试用的BaseMetalake
     */
    public static BaseMetalake createTestMetalake(String name) {
        return BaseMetalake.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName(name)
                .withAuditInfo(
                        AuditInfo.builder()
                                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                                .withCreateTime(Instant.now())
                                .build())
                .withVersion(SchemaVersion.V_0_1)
                .build();
    }

    /**
     * 创建测试用的CatalogEntity
     */
    public static CatalogEntity createTestCatalogEntity(String metalake, String catalogName,
                                                        String provider, Map<String, String> properties) {
        return CatalogEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName(catalogName)
                .withNamespace(NameIdentifier.of(metalake).namespace())
                .withType(Catalog.Type.RELATIONAL)
                .withProvider(provider)
                .withComment("Test catalog")
                .withProperties(properties != null ? properties : Maps.newHashMap())
                .withAuditInfo(
                        AuditInfo.builder()
                                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                                .withCreateTime(Instant.now())
                                .build())
                .build();
    }

    /**
     * 创建包含关键属性的配置
     */
    public static Map<String, String> createCriticalPropertiesConfig() {
        Map<String, String> props = Maps.newHashMap();
        props.put("metastore.uris", "thrift://localhost:9083");
        props.put("jdbc.url", "jdbc:mysql://localhost:3306/test");
        props.put("warehouse", "/tmp/warehouse");
        props.put("authorization.enable", "true");
        return props;
    }

    /**
     * 创建包含非关键属性的配置
     */
    public static Map<String, String> createNonCriticalPropertiesConfig() {
        Map<String, String> props = Maps.newHashMap();
        props.put("description", "Test catalog description");
        props.put("comment", "Test catalog comment");
        props.put("gravitino.catalog.cache.ttl", "3600");
        return props;
    }

    /**
     * 创建混合属性配置
     */
    public static Map<String, String> createMixedPropertiesConfig() {
        Map<String, String> props = Maps.newHashMap();
        props.putAll(createCriticalPropertiesConfig());
        props.putAll(createNonCriticalPropertiesConfig());
        return props;
    }

    /**
     * 验证内存使用是否在合理范围内
     */
    public static boolean isMemoryUsageReasonable(long initialMemory, long currentMemory,
                                                  long maxAllowedIncrease) {
        long memoryIncrease = currentMemory - initialMemory;
        return memoryIncrease <= maxAllowedIncrease;
    }

    /**
     * 强制垃圾回收并等待
     */
    public static void forceGCAndWait() throws InterruptedException {
        System.gc();
        Thread.sleep(100);
        System.gc();
        Thread.sleep(100);
    }

    /**
     * 获取当前内存使用量
     */
    public static long getCurrentMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}