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
package org.apache.gravitino.oss.fs;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.storage.OSSProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOSSFileSystemProvider {

  private final OSSFileSystemProvider provider = new OSSFileSystemProvider();

  @Test
  public void testContainsClientCredentialsTrueWhenBothKeysPresent() {
    Assertions.assertTrue(
        provider.containsClientCredentials(
            ImmutableMap.of(
                OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, "ak",
                OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, "sk")));
  }

  @Test
  public void testContainsClientCredentialsFalseWhenIncomplete() {
    Assertions.assertFalse(
        provider.containsClientCredentials(
            ImmutableMap.of(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, "ak")));
    Assertions.assertFalse(
        provider.containsClientCredentials(
            ImmutableMap.of(OSSProperties.GRAVITINO_OSS_ENDPOINT, "endpoint")));
  }
}
