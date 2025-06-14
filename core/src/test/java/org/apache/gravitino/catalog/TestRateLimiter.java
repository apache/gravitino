package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestRateLimiter {

    @Test
    void testBasicRateLimiting() {
        // Given
        RateLimiter rateLimiter = RateLimiter.create(2.0); // 每秒2个请求

        // When & Then
        assertTrue(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));
        assertTrue(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));
        // 第三个请求应该被限制
        assertFalse(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));
    }

    @Test
    void testRateLimiterRefill() throws InterruptedException {
        // Given
        RateLimiter rateLimiter = RateLimiter.create(1.0); // 每秒1个请求

        // When
        assertTrue(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));
        assertFalse(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));

        // 等待超过1秒让令牌桶重新填充
        Thread.sleep(1100);

        // Then
        assertTrue(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));
    }

    @Test
    void testHighRateLimit() {
        // Given
        RateLimiter rateLimiter = RateLimiter.create(100.0); // 每秒100个请求

        // When & Then - 应该能够快速获取多个许可
        for (int i = 0; i < 50; i++) {
            assertTrue(rateLimiter.tryAcquire(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void testZeroRateLimit() {
        // Given
        RateLimiter rateLimiter = RateLimiter.create(0.1); // 每秒0.1个请求

        // When & Then
        assertTrue(rateLimiter.tryAcquire(1, TimeUnit.SECONDS)); // 第一个请求应该成功
        assertFalse(rateLimiter.tryAcquire(1, TimeUnit.SECONDS)); // 后续请求应该被限制
    }
}