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

import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class TestCredentialCacheKey {

  @Test
  void testCredentialCacheKey() {

    PathBasedCredentialContext context =
        new PathBasedCredentialContext("user1", ImmutableSet.of("path1"), ImmutableSet.of("path2"));
    PathBasedCredentialContext contextWithDiffUser =
        new PathBasedCredentialContext("user2", ImmutableSet.of("path1"), ImmutableSet.of("path2"));
    PathBasedCredentialContext contextWithDiffPath =
        new PathBasedCredentialContext("user1", ImmutableSet.of("path3"), ImmutableSet.of("path4"));

    CredentialCacheKey key1 = new CredentialCacheKey("s3-token", context);

    Set<CredentialCacheKey> cache = ImmutableSet.of(key1);
    Assertions.assertTrue(cache.contains(key1));

    // different user
    CredentialCacheKey key2 = new CredentialCacheKey("s3-token", contextWithDiffUser);
    Assertions.assertFalse(cache.contains(key2));

    // different path
    CredentialCacheKey key3 = new CredentialCacheKey("s3-token", contextWithDiffPath);
    Assertions.assertFalse(cache.contains(key3));

    // different credential type
    CredentialCacheKey key4 = new CredentialCacheKey("s3-token1", context);
    Assertions.assertFalse(cache.contains(key4));
  }

  @Test
  void testToStringContainsSpaceBetweenFields() {
    PathBasedCredentialContext context =
        new PathBasedCredentialContext("user", ImmutableSet.of(), ImmutableSet.of());
    CredentialCacheKey key = new CredentialCacheKey("s3-token", context);

    Assertions.assertTrue(key.toString().contains("credentialType: s3-token credentialContext:"));
  }
}
