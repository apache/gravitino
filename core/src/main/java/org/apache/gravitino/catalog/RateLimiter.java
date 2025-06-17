package org.apache.gravitino.catalog;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简单的频率限制器实现
 */
public class RateLimiter {
    private final double permitsPerSecond;
    private final AtomicLong lastRefillTime;
    private final AtomicLong availablePermits;
    private final long maxPermits;

    private RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.maxPermits = (long) Math.max(1, permitsPerSecond);
        this.availablePermits = new AtomicLong(maxPermits);
        this.lastRefillTime = new AtomicLong(System.nanoTime());
    }

    public static RateLimiter create(double permitsPerSecond) {
        return new RateLimiter(permitsPerSecond);
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
        refill();
        return availablePermits.getAndDecrement() > 0;
    }

    private void refill() {
        long now = System.nanoTime();
        long lastRefill = lastRefillTime.get();

        if (now > lastRefill) {
            long elapsedNanos = now - lastRefill;
            double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
            long permitsToAdd = (long) (elapsedSeconds * permitsPerSecond);

            if (permitsToAdd > 0 && lastRefillTime.compareAndSet(lastRefill, now)) {
                long currentPermits = availablePermits.get();
                long newPermits = Math.min(maxPermits, currentPermits + permitsToAdd);
                availablePermits.set(newPermits);
            }
        }
    }
}