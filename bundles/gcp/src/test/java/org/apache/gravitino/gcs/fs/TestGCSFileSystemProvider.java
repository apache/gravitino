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
package org.apache.gravitino.gcs.fs;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.storage.GCSProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGCSFileSystemProvider {

  private final GCSFileSystemProvider provider = new GCSFileSystemProvider();

  @Test
  public void testContainsClientCredentialsTrueWhenServiceAccountFilePresent() {
    Assertions.assertTrue(
        provider.containsClientCredentials(
            ImmutableMap.of(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, "/path/to/file")));
  }

  @Test
  public void testContainsClientCredentialsFalseWhenAbsent() {
    Assertions.assertFalse(provider.containsClientCredentials(ImmutableMap.of("other", "value")));
  }
}
