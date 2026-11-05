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
package org.apache.gravitino.storage.relational;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Regression probe for change-log identity allocation versus transaction commit order. */
public class TestCacheCommitOrdering extends TestJDBCBackend {
  @TestTemplate
  void testLowerChangeIdCommittedLateMustReachPeerCache() throws Exception {
    createParentEntities("lake", "order_catalog", "schema", AUDIT_INFO);
    Namespace namespace = Namespace.of("lake", "order_catalog", "schema");
    createAndInsertSchema("lake", "order_catalog", "fast_schema");
    Namespace fastNamespace = Namespace.of("lake", "order_catalog", "fast_schema");
    TableEntity slow =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "slow", AUDIT_INFO);
    TableEntity fast =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), fastNamespace, "fast", AUDIT_INFO);
    backend.insert(slow, false);
    backend.insert(fast, false);
    CaffeineEntityCache peer = new CaffeineEntityCache(new Config() {});
    peer.put(slow);
    peer.put(fast);
    // The hour-long poll interval also spaces replay sweeps an hour apart; skip ahead before the
    // polls that must start a new sweep after the late commit.
    AtomicLong clockOffset = new AtomicLong();
    EntityChangeLogPoller poller = spy(new EntityChangeLogPoller(3600));
    doAnswer(ignored -> System.nanoTime() + clockOffset.get()).when(poller).nanoTime();
    poller.registerListener(new EntityCacheChangeLogListener(peer));
    poller.start();
    CountDownLatch insertedLowId = new CountDownLatch(1);
    CountDownLatch commitSlow = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      Future<?> transaction =
          writer.submit(
              () -> {
                SessionUtils.beginTransaction();
                boolean committed = false;
                try {
                  backend.update(
                      slow.nameIdentifier(),
                      EntityType.TABLE,
                      entity ->
                          createTableEntity(slow.id(), namespace, "slow_renamed", AUDIT_INFO));
                  insertedLowId.countDown();
                  Assertions.assertTrue(commitSlow.await(15, TimeUnit.SECONDS));
                  SessionUtils.commitTransaction();
                  committed = true;
                  return null;
                } finally {
                  if (!committed) {
                    SessionUtils.rollbackTransaction();
                  }
                }
              });
      Assertions.assertTrue(insertedLowId.await(15, TimeUnit.SECONDS));
      backend.update(
          fast.nameIdentifier(),
          EntityType.TABLE,
          entity -> createTableEntity(fast.id(), fastNamespace, "fast_renamed", AUDIT_INFO));
      poller.pollChanges();
      Assertions.assertFalse(peer.contains(fast.nameIdentifier(), EntityType.TABLE));
      Assertions.assertTrue(peer.contains(slow.nameIdentifier(), EntityType.TABLE));
      commitSlow.countDown();
      transaction.get(15, TimeUnit.SECONDS);
      Assertions.assertFalse(backend.exists(slow.nameIdentifier(), EntityType.TABLE));
      clockOffset.set(TimeUnit.HOURS.toNanos(2));
      for (int i = 0; i < 3; i++) {
        poller.pollChanges();
      }
      Assertions.assertFalse(
          peer.contains(slow.nameIdentifier(), EntityType.TABLE),
          "A committed low-ID invalidation must not be skipped after consuming a higher ID");
    } finally {
      commitSlow.countDown();
      writer.shutdownNow();
      try {
        Assertions.assertTrue(writer.awaitTermination(15, TimeUnit.SECONDS));
      } finally {
        poller.close();
        peer.clear();
      }
    }
  }
}
