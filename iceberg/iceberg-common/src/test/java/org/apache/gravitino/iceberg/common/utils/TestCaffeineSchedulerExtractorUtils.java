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
package org.apache.gravitino.iceberg.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.Test;

/** Test class for CaffeineSchedulerExtractorUtils. */
public class TestCaffeineSchedulerExtractorUtils {

  @Test
  public void testExtractSchedulerExecutor() {
    ScheduledExecutorService testScheduler = ThreadPools.newScheduledPool("test-extract", 1);
    Cache<String, String> cache =
        Caffeine.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .scheduler(Scheduler.forScheduledExecutorService(testScheduler))
            .build();

    try {
      // Use CaffeineSchedulerExtractorUtils to extract scheduler
      ScheduledExecutorService extracted =
          CaffeineSchedulerExtractorUtils.extractSchedulerExecutor(cache);
      assertNotNull(extracted, "Should successfully extract scheduler from cache");

      // Verification 1: Reference equality - they are the SAME object
      assertSame(
          testScheduler,
          extracted,
          "Extracted scheduler MUST be the same instance as the one we provided");

      // Verification 2: Initial state - both should NOT be shutdown
      assertFalse(testScheduler.isShutdown(), "Original should not be shutdown initially");
      assertFalse(extracted.isShutdown(), "Extracted should not be shutdown initially");

      // Verification 3: Shutdown extracted and verify original is also shutdown
      extracted.shutdownNow();

      // Both should be shutdown (proving they are the SAME object)
      assertTrue(extracted.isShutdown(), "Extracted should be shutdown");
      assertTrue(testScheduler.isShutdown(), "Original MUST be shutdown, proving same instance!");

    } catch (Exception e) {
      testScheduler.shutdownNow();
      throw e;
    }
  }

  @Test
  public void testGetPacerFromCache() throws Exception {
    ScheduledExecutorService testScheduler = ThreadPools.newScheduledPool("test-pacer", 1);

    Cache<String, String> cache =
        Caffeine.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .scheduler(Scheduler.forScheduledExecutorService(testScheduler))
            .build();

    try {
      // Unwrap cache using utility method
      Object cacheImpl = CaffeineSchedulerExtractorUtils.unwrapCache(cache);
      assertNotNull(cacheImpl, "Should unwrap cache");

      // Get Pacer using utility method
      Object pacer = CaffeineSchedulerExtractorUtils.getPacerFromCache(cacheImpl);
      assertNotNull(pacer, "Should successfully get Pacer object");
      assertEquals("Pacer", pacer.getClass().getSimpleName(), "Should be Pacer class");

    } finally {
      testScheduler.shutdownNow();
    }
  }

  /** Test the complete extraction chain with identity verification at each step. */
  @Test
  public void testCompleteExtractionChain() throws Exception {
    ScheduledExecutorService testScheduler = ThreadPools.newScheduledPool("test-chain", 1);

    Cache<String, String> cache =
        Caffeine.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .scheduler(Scheduler.forScheduledExecutorService(testScheduler))
            .build();

    try {
      // Step 1: Unwrap cache
      Object cacheImpl = CaffeineSchedulerExtractorUtils.unwrapCache(cache);
      assertNotNull(cacheImpl, "Step 1: Should unwrap cache");

      // Step 2: Get Pacer
      Object pacer = CaffeineSchedulerExtractorUtils.getPacerFromCache(cacheImpl);
      assertNotNull(pacer, "Step 2: Should get Pacer");
      assertEquals("Pacer", pacer.getClass().getSimpleName(), "Step 2: Should be Pacer class");

      // Step 3: Get Scheduler from Pacer
      Scheduler scheduler = CaffeineSchedulerExtractorUtils.getSchedulerFromPacer(pacer);
      assertNotNull(scheduler, "Step 3: Should get Scheduler");

      // Step 4: Get ExecutorService from Scheduler
      ScheduledExecutorService executor =
          CaffeineSchedulerExtractorUtils.getExecutorServiceFromScheduler(scheduler);
      assertNotNull(executor, "Step 4: Should get ScheduledExecutorService");

      // Step 5: CRITICAL - Identity verification
      assertSame(testScheduler, executor, "Extracted MUST be same instance!");

      // Step 6: Behavior verification - shutdown one affects the other
      assertFalse(testScheduler.isShutdown(), "Original should not be shutdown initially");
      assertFalse(executor.isShutdown(), "Extracted should not be shutdown initially");

      executor.shutdownNow();

      assertTrue(executor.isShutdown(), "Extracted should be shutdown");
      assertTrue(testScheduler.isShutdown(), "Original MUST be shutdown - same object!");

    } catch (Exception e) {
      testScheduler.shutdownNow();
      throw e;
    }
  }
}
