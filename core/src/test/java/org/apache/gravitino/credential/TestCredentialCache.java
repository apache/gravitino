/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCredentialCache {

  @Test
  void testAbsentCredentialIsNotCached() throws IOException {
    try (CredentialCache<String> cache = new CredentialCache<>()) {
      cache.initialize(ImmutableMap.of());

      AtomicInteger supplierCalls = new AtomicInteger();
      Assertions.assertFalse(
          cache
              .getCredential(
                  "key",
                  key -> {
                    supplierCalls.incrementAndGet();
                    return Optional.empty();
                  })
              .isPresent());

      // The absent credential expires immediately, so a second lookup invokes the supplier again.
      Assertions.assertFalse(
          cache
              .getCredential(
                  "key",
                  key -> {
                    supplierCalls.incrementAndGet();
                    return Optional.empty();
                  })
              .isPresent());

      Assertions.assertEquals(2, supplierCalls.get());
    }
  }

  @Test
  void testPresentCredentialIsCached() throws IOException {
    try (CredentialCache<String> cache = new CredentialCache<>()) {
      cache.initialize(ImmutableMap.of());

      Credential credential = Mockito.mock(Credential.class);
      Mockito.when(credential.expireTimeInMs())
          .thenReturn(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

      AtomicInteger supplierCalls = new AtomicInteger();
      for (int i = 0; i < 2; i++) {
        Optional<Credential> cached =
            cache.getCredential(
                "key",
                key -> {
                  supplierCalls.incrementAndGet();
                  return Optional.of(credential);
                });
        Assertions.assertTrue(cached.isPresent());
        Assertions.assertSame(credential, cached.get());
      }

      // The present credential is cached, so the supplier is only invoked once.
      Assertions.assertEquals(1, supplierCalls.get());
    }
  }
}
