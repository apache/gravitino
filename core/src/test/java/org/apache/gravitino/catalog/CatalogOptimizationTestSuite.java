package org.apache.gravitino.catalog;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Catalog优化方案测试套件
 */
@Suite
@SelectClasses({
        TestClassLoaderPool.class,
        TestCatalogPropertyAnalyzer.class,
        TestRateLimiter.class,
        TestCatalogMetrics.class,
        TestCatalogWrapperEnhancement.class,
        TestCatalogManagerOptimization.class,
        TestCatalogPerformanceBenchmark.class,
        TestCatalogConfigValidation.class,
        TestMetaspaceOOMScenario.class,
        TestCatalogOptimizationE2E.class
})
public class CatalogOptimizationTestSuite {
    // 测试套件配置类，用于统一运行所有相关测试
}