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
package org.apache.gravitino.idp.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestVerifiedBasicCredentialCache {

  @Test
  public void testDisabledCacheNeverRemembers() {
    VerifiedBasicCredentialCache cache = new VerifiedBasicCredentialCache(0, 100);
    assertFalse(cache.isEnabled());
    cache.rememberSuccess("alice", "Passw0rd-1234", "hash-1");
    assertFalse(cache.isVerified("alice", "Passw0rd-1234", "hash-1"));
    assertEquals(0, cache.estimatedSize());
  }

  @Test
  public void testRememberAndVerifySuccess() {
    VerifiedBasicCredentialCache cache = new VerifiedBasicCredentialCache(60, 100);
    assertTrue(cache.isEnabled());
    cache.rememberSuccess("alice", "Passw0rd-1234", "hash-1");
    assertTrue(cache.isVerified("alice", "Passw0rd-1234", "hash-1"));
    assertFalse(cache.isVerified("alice", "Passw0rd-1234", "hash-2"));
    assertFalse(cache.isVerified("alice", "Wrong-Password1!", "hash-1"));
    assertFalse(cache.isVerified("bob", "Passw0rd-1234", "hash-1"));
  }

  @Test
  public void testInvalidateUserDropsCachedCredential() {
    VerifiedBasicCredentialCache cache = new VerifiedBasicCredentialCache(60, 100);
    cache.rememberSuccess("alice", "Passw0rd-1234", "hash-1");
    cache.invalidateUser("alice");
    assertFalse(cache.isVerified("alice", "Passw0rd-1234", "hash-1"));
  }

  @Test
  public void testPasswordChangeReplacesPreviousCredentialKey() {
    VerifiedBasicCredentialCache cache = new VerifiedBasicCredentialCache(60, 100);
    cache.rememberSuccess("alice", "Passw0rd-1234", "hash-1");
    cache.rememberSuccess("alice", "New-Password1!", "hash-2");
    assertFalse(cache.isVerified("alice", "Passw0rd-1234", "hash-1"));
    assertTrue(cache.isVerified("alice", "New-Password1!", "hash-2"));
    assertEquals(1, cache.estimatedSize());
  }

  @Test
  public void testCredentialKeyIsStableAndDistinct() {
    String first = VerifiedBasicCredentialCache.credentialKey("alice", "Passw0rd-1234");
    String second = VerifiedBasicCredentialCache.credentialKey("alice", "Passw0rd-1234");
    String other = VerifiedBasicCredentialCache.credentialKey("alice", "Other-Password1!");
    assertEquals(first, second);
    assertNotEquals(first, other);
  }
}
