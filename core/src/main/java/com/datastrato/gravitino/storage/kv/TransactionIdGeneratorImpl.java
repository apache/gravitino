/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

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
  public static final Logger LOGGER = LoggerFactory.getLogger(TransactionIdGeneratorImpl.class);

  private final KvBackend kvBackend;
  public static final String LAST_ID = "last_timestamp";
  private long incrementId = 0L;

  private final ScheduledExecutorService idSaverScheduleExecutor =
      new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("testTransactionIdGenerator-%d")
              .build());

  public TransactionIdGeneratorImpl(KvBackend kvBackend) {
    this.kvBackend = kvBackend;
    init();
    idSaverScheduleExecutor.schedule(
        () -> {
          int i = 0;
          while (i++ < 3) {
            try {
              kvBackend.put(
                  LAST_ID.getBytes(StandardCharsets.UTF_8),
                  ByteUtils.longToByte(System.currentTimeMillis()),
                  true);
              return;
            } catch (IOException e) {
              LOGGER.warn("Failed to initialize transaction id generator, retrying...", e);
            }
          }

          if (i == 3) {
            throw new RuntimeException(
                "Failed to initialize transaction id generator after 3 retries");
          }
        },
        5,
        TimeUnit.SECONDS);
  }

  private void init() {
    long current = System.currentTimeMillis();
    long old;
    try {
      old = getSavedTs();

      // In case of time skew, we will wait at 5 seconds
      int retries = 0;
      while (current <= old + 5000 && retries++ < 50) {
        Thread.sleep(100);
        current = System.currentTimeMillis();
      }

      if (current <= old + 5000) {
        throw new RuntimeException("Failed to initialize transaction id generator after 5 seconds");
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private long getSavedTs() throws IOException {
    byte[] oldIdBytes = kvBackend.get(LAST_ID.getBytes(StandardCharsets.UTF_8));
    return oldIdBytes == null ? 0 : ByteUtils.byteToLong(oldIdBytes);
  }

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
