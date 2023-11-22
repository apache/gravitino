/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.datastrato.gravitino.utils.ByteUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionIdGeneratorImpl implements TransactionIdGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionIdGeneratorImpl.class);

  private final KvBackend kvBackend;
  private static final String LAST_TIMESTAMP = "last_timestamp";
  private volatile long incrementId = 0L;
  private final Config config;

  public TransactionIdGeneratorImpl(KvBackend kvBackend, Config config) {
    this.kvBackend = kvBackend;
    this.config = config;
  }

  public void start() {
    long maxSkewTime = config.get(Configs.STORE_TRANSACTION_MAX_SKEW_TIME);
    checkTimeSkew(maxSkewTime);

    ScheduledExecutorService idSaverScheduleExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("TransactionIdGenerator-thread-%d")
                .setUncaughtExceptionHandler(
                    (t, e) -> LOGGER.error("Uncaught exception in thread {}", t, e))
                .build());

    idSaverScheduleExecutor.schedule(
        () -> {
          int i = 0;
          while (i++ < 3) {
            try {
              kvBackend.put(
                  LAST_TIMESTAMP.getBytes(StandardCharsets.UTF_8),
                  ByteUtils.longToByte(System.currentTimeMillis()),
                  true);
              return;
            } catch (IOException e) {
              LOGGER.warn("Failed to save current timestamp to storage layer, retrying...", e);
            }
          }

          throw new RuntimeException(
              "Failed to save current timestamp to storage layer after 3 retries, please check"
                  + "whether the storage layer is healthy.");
        },
        maxSkewTime,
        TimeUnit.SECONDS);
  }

  private void checkTimeSkew(long maxSkewTimeInSecond) {
    long current = System.currentTimeMillis();
    long old;
    try {
      old = getSavedTs();
      long maxSkewTimeInMills = maxSkewTimeInSecond * 1000;
      // In case of time skew, we will wait maxSkewTimeInSecond(default 2) seconds.
      int retries = 0;
      while (current <= old + maxSkewTimeInMills && retries++ < maxSkewTimeInMills / 100) {
        Thread.sleep(100);
        current = System.currentTimeMillis();
      }

      if (current <= old + maxSkewTimeInSecond * 1000) {
        throw new RuntimeException(
            String.format(
                "Failed to initialize transaction id generator after %d seconds, time skew is too large",
                maxSkewTimeInSecond));
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the last saved timestamp from the storage layer. We will store the current timestamp to
   * storage layer every 2(default) seconds.
   */
  private long getSavedTs() throws IOException {
    byte[] oldIdBytes = kvBackend.get(LAST_TIMESTAMP.getBytes(StandardCharsets.UTF_8));
    return oldIdBytes == null ? 0 : ByteUtils.byteToLong(oldIdBytes);
  }

  /**
   * We use the timestamp as the high 48 bits and the incrementId as the low 16 bits. The timestamp
   * is always incremental.
   */
  @Override
  public synchronized long nextId() {
    try {
      long current = System.currentTimeMillis();
      incrementId++;
      if (incrementId >= (1 << 18 - 1)) {
        Thread.sleep(1);
        current = System.currentTimeMillis();
        incrementId = 0;
      }
      return (current << 18) + incrementId;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
