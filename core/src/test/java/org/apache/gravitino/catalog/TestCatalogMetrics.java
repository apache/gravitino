package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCatalogMetrics {

    private CatalogMetrics catalogMetrics;

    @BeforeEach
    void setUp() {
        catalogMetrics = new CatalogMetrics();
    }

    @Test
    void testRecordClassLoaderCreation() {
        // When
        catalogMetrics.recordClassLoaderCreation();
        catalogMetrics.recordClassLoaderCreation();

        // Then - 应该能够正常记录，不抛出异常
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());
    }

    @Test
    void testRecordClassLoaderRelease() {
        // When
        catalogMetrics.recordClassLoaderRelease();

        // Then
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());
    }

    @Test
    void testRecordHotUpdate() {
        // When
        catalogMetrics.recordHotUpdate();

        // Then
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());
    }

    @Test
    void testRecordFullUpdate() {
        // When
        catalogMetrics.recordFullUpdate();

        // Then
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());
    }

    @Test
    void testRecordRateLimitHit() {
        // When
        catalogMetrics.recordRateLimitHit();

        // Then
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());
    }

    @Test
    void testMultipleOperationsRecording() {
        // When - 记录多种操作
        catalogMetrics.recordClassLoaderCreation();
        catalogMetrics.recordClassLoaderCreation();
        catalogMetrics.recordClassLoaderRelease();
        catalogMetrics.recordHotUpdate();
        catalogMetrics.recordFullUpdate();
        catalogMetrics.recordRateLimitHit();

        // Then - 应该能够正常记录所有操作
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());
    }

    @Test
    void testConcurrentMetricsRecording() throws InterruptedException {
        // Given
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // When - 并发记录指标
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    catalogMetrics.recordClassLoaderCreation();
                    catalogMetrics.recordHotUpdate();
                    catalogMetrics.recordFullUpdate();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then - 所有线程应该完成
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> catalogMetrics.logMetrics());

        executor.shutdown();
    }
}