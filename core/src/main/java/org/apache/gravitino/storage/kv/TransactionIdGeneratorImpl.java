/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.kv;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.TransactionIdGenerator;
import org.apache.gravitino.utils.ByteUtils;
import org.apache.gravitino.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionIdGeneratorImpl implements TransactionIdGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionIdGeneratorImpl.class);

  private final KvBackend kvBackend;
  // We use three control characters 0x1D, 0x00, 0x01 to separate it from other keys.
  private static final byte[] ID_GENERATOR_PREFIX = new byte[] {0x1D, 0x00, 0x01};

  static final byte[] LAST_TIMESTAMP =
      Bytes.concat(ID_GENERATOR_PREFIX, "last_timestamp".getBytes(StandardCharsets.UTF_8));

  private volatile long incrementId = 0L;
  private volatile long lastTransactionId = 0L;
  private final Config config;

  private final ScheduledExecutorService scheduledThreadPoolExecutor =
      new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("TransactionIdGenerator-thread-%d")
              .setUncaughtExceptionHandler(
                  (t, e) -> LOGGER.error("Uncaught exception in thread {}", t, e))
              .build());

  public TransactionIdGeneratorImpl(KvBackend kvBackend, Config config) {
    this.kvBackend = kvBackend;
    this.config = config;
  }

  @Override
  public void start() {
    long maxSkewTime = config.get(Configs.STORE_TRANSACTION_MAX_SKEW_TIME);
    // Why use maxSkewTime + 1? Because we will save the current timestamp to storage layer every
    // maxSkewTime second and the save operation will also take a moment. Usually, it takes less
    // than 1 millisecond, so we'd better wait maxSkewTime + 1000 millisecond to make sure the
    // timestamp is OK.
    checkTimeSkew(maxSkewTime + 1000);

    scheduledThreadPoolExecutor.scheduleAtFixedRate(
        () -> {
          int i = 0;
          while (i++ < 3) {
            try {
              kvBackend.put(LAST_TIMESTAMP, ByteUtils.longToByte(System.currentTimeMillis()), true);
              return;
            } catch (IOException e) {
              LOGGER.warn("Failed to save current timestamp to storage layer, retrying...", e);
            }
          }

          throw new RuntimeException(
              "Failed to save current timestamp to storage layer after 3 retries, please check"
                  + "whether the storage layer is healthy.");
        },
        maxSkewTime * 2,
        maxSkewTime,
        TimeUnit.MILLISECONDS);
  }

  private void checkTimeSkew(long maxSkewTimeInMs) {
    long current = System.currentTimeMillis();
    long old;
    try {
      old = getSavedTs();
      // In case of time skew, we will wait maxSkewTimeInSecond(default 2) seconds.
      int retries = 0;
      while (current <= old + maxSkewTimeInMs && retries++ < maxSkewTimeInMs / 100) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread was interrupted, exception: ", e);
        }
        current = System.currentTimeMillis();
      }

      if (current <= old + maxSkewTimeInMs) {
        throw new RuntimeException(
            String.format(
                "Failed to initialize transaction id generator after %d milliseconds, time skew is too large",
                maxSkewTimeInMs));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the last saved timestamp from the storage layer. We will store the current timestamp to
   * storage layer every 2(default) seconds.
   */
  private long getSavedTs() throws IOException {
    byte[] oldIdBytes = kvBackend.get(LAST_TIMESTAMP);
    return oldIdBytes == null ? 0 : ByteUtils.byteToLong(oldIdBytes);
  }

  /**
   * We use the timestamp as the high 46 bits and the incrementId as the low 18 bits. The timestamp
   * is always incremental.
   */
  @Override
  public synchronized long nextId() {
    incrementId++;
    if (incrementId >= ((1 << 18) - 1)) {
      incrementId = 0;
    }

    while (true) {
      long tmpId = (System.currentTimeMillis() << 18) + incrementId;
      if (tmpId > lastTransactionId) {
        lastTransactionId = tmpId;
        return tmpId;
      }
    }
  }

  @Override
  public void close() throws IOException {
    scheduledThreadPoolExecutor.shutdownNow();
    try {
      scheduledThreadPoolExecutor.awaitTermination(2000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Failed to close thread pool scheduledThreadPoolExecutor in TransactionIdGeneratorImpl with in 2000 milliseconds",
          e);
    }
  }
}
