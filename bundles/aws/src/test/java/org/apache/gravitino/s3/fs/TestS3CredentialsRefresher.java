/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.s3.fs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.s3.fs.S3CredentialsRefresher.S3Credentials;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests the refresh and expiration logic shared by the AWS SDK v1 and v2 credential providers. */
public class TestS3CredentialsRefresher {

  private static final List<Credential> SERVED = new ArrayList<>();
  private static final AtomicInteger FETCHES = new AtomicInteger();
  private static volatile long fetchDelayMs;

  @BeforeEach
  void setUp() {
    SERVED.clear();
    FETCHES.set(0);
    fetchDelayMs = 0;
  }

  @AfterEach
  void tearDown() {
    SERVED.clear();
  }

  @Test
  void testCredentialWithoutExpirationIsFetchedOnce() {
    SERVED.add(new S3SecretKeyCredential("access-key", "secret-key"));
    S3CredentialsRefresher refresher = newRefresher();

    S3Credentials first = refresher.getCredentials();
    S3Credentials second = refresher.getCredentials();

    Assertions.assertEquals(1, FETCHES.get());
    Assertions.assertEquals("access-key", first.accessKeyId());
    Assertions.assertEquals("secret-key", first.secretAccessKey());
    Assertions.assertNull(first.sessionToken());
    Assertions.assertSame(first, second);
  }

  @Test
  void testExplicitRefreshFetchesAgain() {
    SERVED.add(new S3SecretKeyCredential("access-key-1", "secret-key-1"));
    S3CredentialsRefresher refresher = newRefresher();
    Assertions.assertEquals("access-key-1", refresher.getCredentials().accessKeyId());

    SERVED.set(0, new S3SecretKeyCredential("access-key-2", "secret-key-2"));
    refresher.refresh();

    Assertions.assertEquals(2, FETCHES.get());
    Assertions.assertEquals("access-key-2", refresher.getCredentials().accessKeyId());
    Assertions.assertEquals(2, FETCHES.get());
  }

  @Test
  void testExpiredCredentialIsRefreshed() {
    SERVED.add(
        new S3TokenCredential(
            "access-key-1", "secret-key-1", "token-1", System.currentTimeMillis() - 1000L));
    S3CredentialsRefresher refresher = newRefresher();
    Assertions.assertEquals("token-1", refresher.getCredentials().sessionToken());
    Assertions.assertEquals(1, FETCHES.get());

    SERVED.set(
        0,
        new S3TokenCredential(
            "access-key-2", "secret-key-2", "token-2", System.currentTimeMillis() + 3_600_000L));
    S3Credentials refreshed = refresher.getCredentials();

    Assertions.assertEquals(2, FETCHES.get());
    Assertions.assertEquals("access-key-2", refreshed.accessKeyId());
    Assertions.assertEquals("token-2", refreshed.sessionToken());

    // The new credential is valid for another hour, so it is served from the cache.
    Assertions.assertSame(refreshed, refresher.getCredentials());
    Assertions.assertEquals(2, FETCHES.get());
  }

  @Test
  void testRefreshesAtHalfOfTheLifetime() {
    long lifetimeMs = 100_000L;
    long before = System.currentTimeMillis();
    SERVED.add(new S3TokenCredential("access-key", "secret-key", "token", before + lifetimeMs));
    S3CredentialsRefresher refresher = newRefresher();
    refresher.getCredentials();
    long after = System.currentTimeMillis();

    long refreshTime = refresher.expirationTime();
    Assertions.assertTrue(refreshTime >= before + lifetimeMs / 2 - 1000L);
    Assertions.assertTrue(refreshTime <= after + lifetimeMs / 2);
    // Not refreshed yet.
    refresher.getCredentials();
    Assertions.assertEquals(1, FETCHES.get());
  }

  @Test
  void testConcurrentCallersFetchOnce() throws Exception {
    SERVED.add(
        new S3TokenCredential(
            "access-key", "secret-key", "token", System.currentTimeMillis() + 3_600_000L));
    // Keep the first fetch running while the other callers wait for the lock.
    fetchDelayMs = 200;
    S3CredentialsRefresher refresher = newRefresher();
    int callers = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(callers);
    try {
      List<Future<S3Credentials>> results = new ArrayList<>();
      for (int i = 0; i < callers; i++) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  return refresher.getCredentials();
                }));
      }
      start.countDown();
      for (Future<S3Credentials> result : results) {
        Assertions.assertEquals("token", result.get(30, TimeUnit.SECONDS).sessionToken());
      }
    } finally {
      executor.shutdownNow();
    }
    Assertions.assertEquals(1, FETCHES.get());
  }

  @Test
  void testNoSuitableCredential() {
    S3CredentialsRefresher refresher = newRefresher();

    RuntimeException e = Assertions.assertThrows(RuntimeException.class, refresher::getCredentials);
    Assertions.assertTrue(e.getMessage().contains("No suitable credential for S3"));
  }

  private S3CredentialsRefresher newRefresher() {
    Configuration conf = new Configuration(false);
    conf.set(
        GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
        ListCredentialsProvider.class.getName());
    return new S3CredentialsRefresher(conf);
  }

  /** Serves the credentials registered in {@link #SERVED} and counts the fetches. */
  public static class ListCredentialsProvider implements GravitinoFileSystemCredentialsProvider {
    private Configuration conf;

    @Override
    public Credential[] getCredentials() {
      FETCHES.incrementAndGet();
      if (fetchDelayMs > 0) {
        try {
          Thread.sleep(fetchDelayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return SERVED.toArray(new Credential[0]);
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
