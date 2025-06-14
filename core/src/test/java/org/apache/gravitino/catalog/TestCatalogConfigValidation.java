package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Test;

public class TestCatalogConfigValidation {

    @Test
    void testClassLoaderPoolConfiguration() {
        // Given
        Config config = new Config(false) {
        };
        config.set(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE, 100L);
        config.set(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES, 60L);

        // When & Then
        assertEquals(100L, config.get(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE));
        assertEquals(60L, config.get(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES));
    }

    @Test
    void testRateLimitConfiguration() {
        // Given
        Config config = new Config(false) {
        };
        config.set(Configs.CATALOG_OPERATIONS_PER_SECOND, 50.0);

        // When & Then
        assertEquals(50.0, config.get(Configs.CATALOG_OPERATIONS_PER_SECOND));
    }

    @Test
    void testDefaultConfigurationValues() {
        // Given
        Config config = new Config(false) {
        };

        // When & Then - 验证默认值
        assertEquals(50L, config.get(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE));
        assertEquals(30L, config.get(Configs.CATALOG_CLASSLOADER_POOL_EXPIRE_MINUTES));
        assertEquals(10.0, config.get(Configs.CATALOG_OPERATIONS_PER_SECOND));
    }

    @Test
    void testInvalidConfigurationValues() {
        // Given
        Config config = new Config(false) {
        };

        // When & Then - 测试边界值
        assertDoesNotThrow(() -> config.set(Configs.CATALOG_CLASSLOADER_POOL_MAX_SIZE, 1L));
        assertDoesNotThrow(() -> config.set(Configs.CATALOG_OPERATIONS_PER_SECOND, 0.1));
    }
}